"""Opt-in glue to existing TickFlow writer and StockData reader. No downloader."""
from pathlib import Path
import json
import logging
import sys
import threading
from app.services import forward_provenance_v1 as fp

CONTROL=Path('/home/y/trading_core/data/core/forward_provenance_v1/activation.json')
LOCK=threading.RLock()
_manager=None
_reader=None
logger=logging.getLogger(__name__)


def control():
    if not CONTROL.exists():return None
    c=json.loads(CONTROL.read_text())
    if not c.get('active'):return None
    if c['schema_version']!=fp.VERSION:raise ValueError('Unexpected provenance epoch')
    for ref in c['reviewed_files']:
        if fp.sha(ref['path'])!=ref['sha256']:raise ValueError('Forward build disk drift')
    return c


def functions(role):
    from app.services.stockdata.sources import DataSources
    from app.services.stockdata import handlers
    if role=='reader':return {'get_daily':DataSources.get_daily,'h_get_price':handlers.h_get_price,'h_get_forward_provenance':handlers.h_get_forward_provenance,'verify_row':fp.verify_row}
    from app.services import kline_sync
    from app.tickflow.repository import KlineRepository
    from tickflow.resources.klines import _klines_to_dataframe
    return {'normalize_daily':kline_sync._normalize_daily,'write_daily':KlineRepository._write_daily_partition,'sdk_to_dataframe':_klines_to_dataframe,'filter_halt_days':kline_sync.filter_halt_days,'capture':fp.Manager.capture,'publish':fp.Manager.publish}


def manager():
    global _manager
    c=control()
    if c is None:return None
    with LOCK:
        if _manager is None:
            store=fp.Store(c['store_root']);att=fp.attest(store,c,'writer',functions('writer'),{'autonomous_update_not_invoked_by_helper':True})
            _manager=fp.Manager(store,c,c['contracts'],att,fp.code_hash(functions('writer')['normalize_daily']))
        if {k:fp.code_hash(f) for k,f in functions('writer').items()}!=c['build']['roles']['writer']:raise ValueError('Writer loaded code drift')
        return _manager


def install(client):
    m=manager()
    if m is None:return
    hooks=client._client._client.event_hooks.setdefault('response',[])
    if any(getattr(f,'_forward_v1_hook',False) for f in hooks):return
    def capture(response):
        try:m.capture(response)
        except Exception as exc:logger.warning('Forward response evidence unavailable: %s',type(exc).__name__)
    capture._forward_v1_hook=True
    hooks.append(capture)


def dataframe_result(result):
    # Same official SDK conversion, with no catalog/name lookup (normalizer drops name).
    if manager() is None:return result
    from tickflow.resources.klines import _klines_to_dataframe
    return {sym:_klines_to_dataframe(values,symbol=sym,name=None) for sym,values in result.items()}


def bind(frame):
    try:
        m=manager()
        if m is not None:m.bind(frame.to_dicts())
    except Exception as exc:logger.warning('Forward normalization evidence unavailable: %s',type(exc).__name__)


def before_write(path,table,existing,incoming):
    c=control()
    if c is None or table!='kline_daily':return None
    day=path.parent.name.removeprefix('date=')
    if day<=c['legacy_through']:return {'skip_legacy_write':True}
    try:
        m=manager()
        return {'manager':m,'previous_hash':fp.sha(path) if path.exists() else None,
                'old_rows':existing.to_dicts(),'incoming_rows':incoming.to_dicts()}
    except Exception as exc:
        logger.warning('Forward publication evidence unavailable: %s',type(exc).__name__)
        return None


def after_write(path,table,new_rows,context):
    if not context or context.get('skip_legacy_write'):return
    try:context['manager'].publish(path,table,context['previous_hash'],context['old_rows'],new_rows.to_dicts(),context['incoming_rows'])
    except Exception as exc:logger.warning('Forward publication receipt unavailable: %s',type(exc).__name__)


def start_reader():
    global _reader
    c=control()
    if c is None:return
    store=fp.Store(c['store_root'])
    import os
    try:port=int(os.getenv('STOCKDATA_PORT','') or 3322)
    except (ValueError,TypeError):port=3322
    _reader=fp.attest(store,c,'reader',functions('reader'),{'host':os.getenv('STOCKDATA_HOST','127.0.0.1'),'port':port,'autonomous_sync_enabled':os.getenv('STOCKDATA_AUTONOMOUS_SYNC','1')!='0','protocol':1})


def read_proof(parameters,data_root):
    c=control()
    if c is None:return {'status':'EPOCH_INACTIVE'}
    if _reader is None:return {'status':'LOADED_ATTESTATION_MISSING'}
    if {k:fp.code_hash(f) for k,f in functions('reader').items()}!=c['build']['roles']['reader']:return {'status':'LOADED_CODE_DRIFT'}
    symbol=parameters['symbol'];start=parameters['start'];end=parameters['end']
    from datetime import date,timedelta
    lo,hi=date.fromisoformat(start),date.fromisoformat(end)
    if lo>hi or (hi-lo).days>30 or symbol.rsplit('.',1)[-1] not in ('SH','SZ'):raise ValueError('Bounded explicit stock scope only')
    store=fp.Store(c['store_root']);publications=[]
    day=lo
    while day<=hi:
        part=Path(data_root)/'kline_daily'/('date='+day.isoformat())/'part.parquet'
        if part.exists() and day.isoformat()>c['legacy_through']:
            h=fp.sha(part)
            try:
                pid,pub=store.publication('kline_daily',day.isoformat(),h)
                publications.append({'date':day.isoformat(),'publication':pid,'partition_path':str(part),'partition_sha256':h})
            except (FileNotFoundError,ValueError,TypeError):pass
        day+=timedelta(days=1)
    return {'status':'FORWARD_EVIDENCE_REFERENCES','epoch_id':c['epoch_id'],'reader_attestation':_reader,'publications':publications}
