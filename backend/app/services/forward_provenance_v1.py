"""FORWARD_PROVENANCE_V1: immutable response/row/publication evidence.

Also deployed byte-identically as a small StockData helper. No network, DB,
updater, scheduler or trading invocation. No legacy metadata backfill.
"""
from __future__ import annotations
import base64
import hashlib
import json
import math
import os
import types
import re
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone, date
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
from zoneinfo import ZoneInfo

VERSION = 'FORWARD_PROVENANCE_V1'
FIELDS = ('open','high','low','close','volume','amount')
ZONE = ZoneInfo('Asia/Shanghai')
MAX_RESPONSE = 8 * 1024 * 1024


def now(): return datetime.now(timezone.utc).isoformat()
def canonical(v): return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(v): return hashlib.sha256(canonical(v)).hexdigest()
def sha(p):
    with Path(p).open('rb') as f: return hashlib.file_digest(f,'sha256').hexdigest()
def instant(t):
    d=datetime.fromisoformat(t)
    if d.tzinfo is None: raise ValueError('Aware UTC/offset timestamp required')
    return d.astimezone(timezone.utc)
def code_hash(fn):
    code=fn if isinstance(fn,types.CodeType) else fn.__code__
    def constant(v):
        if isinstance(v,types.CodeType):return ['code',identity(v)]
        if isinstance(v,bytes):return ['bytes',v.hex()]
        if isinstance(v,float):return ['float',v.hex()]
        if isinstance(v,complex):return ['complex',v.real.hex(),v.imag.hex()]
        if isinstance(v,tuple):return ['tuple',[constant(x) for x in v]]
        if isinstance(v,frozenset):return ['frozenset',sorted([constant(x) for x in v],key=lambda x:canonical(x))]
        if v is Ellipsis:return ['ellipsis']
        if v is None or isinstance(v,(str,bool,int)):return [type(v).__name__,v]
        raise ValueError('Unsupported executable constant')
    def identity(c):
        return {'args':c.co_argcount,'posonly':c.co_posonlyargcount,'kwonly':c.co_kwonlyargcount,
            'locals':c.co_nlocals,'stack':c.co_stacksize,'flags':c.co_flags,'name':c.co_name,
            'qualname':c.co_qualname,'bytecode':c.co_code.hex(),'exceptions':getattr(c,'co_exceptiontable',b'').hex(),
            'names':c.co_names,'varnames':c.co_varnames,'freevars':c.co_freevars,'cellvars':c.co_cellvars,
            'constants':[constant(v) for v in c.co_consts]}
    return digest(identity(code))
def row(v):
    r={k:float(v[k]) for k in FIELDS}
    if any(not math.isfinite(x) for x in r.values()): raise ValueError('Nonfinite source row')
    d=v['date'];r.update(symbol=v['symbol'],date=d.isoformat() if hasattr(d,'isoformat') else d)
    date.fromisoformat(r['date']);return r


class Store:
    def __init__(self,root): self.root=Path(root)
    def put(self,value):
        body=canonical(value);h=hashlib.sha256(body).hexdigest();p=self.root/'objects'/h[:2]/(h+'.json')
        p.parent.mkdir(parents=True,exist_ok=True)
        try:
            with p.open('xb') as f:f.write(body);f.flush();os.fsync(f.fileno())
            p.chmod(0o444)
        except FileExistsError:
            if sha(p)!=h:raise ValueError('Immutable evidence corruption')
        return h
    def get(self,h):
        if not isinstance(h,str) or len(h)!=64 or any(c not in '0123456789abcdef' for c in h):raise ValueError('Invalid artifact identity')
        p=self.root/'objects'/h[:2]/(h+'.json')
        if sha(p)!=h:raise ValueError('Evidence hash drift')
        return json.loads(p.read_text())
    def raw(self,body):
        if len(body)>MAX_RESPONSE:raise ValueError('Response capture limit')
        h=hashlib.sha256(body).hexdigest();p=self.root/'raw'/h[:2]/(h+'.body');p.parent.mkdir(parents=True,exist_ok=True)
        try:
            with p.open('xb') as f:f.write(body);f.flush();os.fsync(f.fileno())
            p.chmod(0o444)
        except FileExistsError:
            if sha(p)!=h:raise ValueError('Raw evidence drift')
        return h
    def body(self,h):
        if len(h)!=64 or any(c not in '0123456789abcdef' for c in h):raise ValueError('Invalid raw identity')
        p=self.root/'raw'/h[:2]/(h+'.body')
        if sha(p)!=h:raise ValueError('Raw response drift')
        return p.read_bytes()
    def index(self,dataset,day,partition_hash,receipt):
        if dataset!='kline_daily':return
        date.fromisoformat(day)
        data=canonical({'publication_receipt':receipt,'partition_sha256':partition_hash})
        identity=hashlib.sha256(data).hexdigest()
        folder=self.root/'publications'/dataset/day/partition_hash;folder.mkdir(parents=True,exist_ok=True)
        existing=list(folder.glob('*.json'))
        if existing and (len(existing)!=1 or existing[0].stem!=identity):
            raise ValueError('Partition already has immutable publication; no new vintage')
        p=folder/(identity+'.json')
        try:
            with p.open('xb') as f:f.write(data);f.flush();os.fsync(f.fileno())
            p.chmod(0o444)
        except FileExistsError:
            if sha(p)!=identity:raise ValueError('Publication index drift')
    def publication(self,dataset,day,h):
        folder=self.root/'publications'/dataset/day/h
        files=list(folder.glob('*.json'))
        if len(files)!=1:raise FileNotFoundError('One unambiguous committed publication required')
        p=files[0]
        if sha(p)!=p.stem:raise ValueError('Publication index hash drift')
        r=json.loads(p.read_text());v=self.get(r['publication_receipt'])
        if r['partition_sha256']!=h or v['partition_sha256']!=h:raise ValueError('Publication index binding')
        return r['publication_receipt'],v


def review_documents(contract):
    for ref in contract.get('artifacts',[]):
        if sha(ref['path'])!=ref['sha256']:raise ValueError('Reviewed source contract drift')


def session(contract,exchange,day):
    review_documents(contract)
    if exchange not in ('XSHG','XSHE'):raise ValueError('Exchange not reviewed')
    for scope in contract['scopes']:
        if scope['exchange']==exchange and scope['start']<=day<=scope['end']:
            if day not in scope['expected_sessions']:raise ValueError('Declared non-session')
            return day+'T'+scope['competitive_close']+'+08:00'
    raise ValueError('Official calendar out of reviewed scope')


def attest(store,epoch,role,functions,safe_config):
    actual={name:code_hash(fn) for name,fn in functions.items()}
    expected=epoch['build']['roles'][role]
    if actual!=expected:raise ValueError('Loaded code objects do not match reviewed implementation')
    started=now()
    result={'schema_version':VERSION,'kind':'LOADED_CODE_ATTESTATION','epoch_id':epoch['epoch_id'],
      'role':role,'pid':os.getpid(),'started_at':started,'actual_executed_code_identities':actual,
      'reviewed_build_id':digest(epoch['build']),'service_contract_version':'stockdata-protocol-v1',
      'safe_config':safe_config,'safe_config_hash':digest(safe_config),'environment':epoch['environment']}
    return store.put(result)


class Manager:
    def __init__(self,store,epoch,contracts,attestation,normalizer_identity):
        self.store=store;self.epoch=epoch;self.contracts=contracts;self.attestation=attestation
        self.normalizer_identity=normalizer_identity;self.pending={};self.capture_candidates={}
    def capture(self,response):
        # Real httpx response hook: finishes reading before SDK JSON/DataFrame conversion.
        url=urlsplit(str(response.request.url))
        if url.username or url.password or url.port:return
        if response.status_code!=200 or url.scheme!='https' or url.hostname not in ('api.tickflow.org','free-api.tickflow.org') or url.path!='/v1/klines/batch':return
        q=parse_qs(url.query)
        params={k:q[k][0] for k in ('symbols','period','adjust','count','start_time','end_time') if k in q}
        if params.get('period')!='1d' or params.get('adjust')!='none':return
        response.read();body=response.content
        for key in ('authorization','x-api-key','cookie'):
            value=response.request.headers.get(key)
            if value and len(value)>7 and value.encode() in body:raise ValueError('Response contains credential material')
        data=json.loads(body)
        def secrets(v):
            if isinstance(v,dict):return any(k.lower() in ('token','api_key','apikey','authorization','password','secret','x-api-key') or secrets(x) for k,x in v.items())
            return isinstance(v,list) and any(secrets(x) for x in v)
        if secrets(data):raise ValueError('Sensitive response refused; no credential artifact')
        if data.get('code') not in (None,0,200,'OK','SUCCESS') or not isinstance(data.get('data'),dict):return
        fetched=now();universe=sorted(set(params.get('symbols','').split(',')))
        if not universe or any(not re.fullmatch(r'\d{6}\.(SH|SZ)',s) for s in universe) or any(s not in universe for s in data['data']):return
        try:
            parsed_http_date=parsedate_to_datetime(response.headers.get('date',''))
            http_date=parsed_http_date.isoformat() if parsed_http_date.tzinfo is not None else 'UNKNOWN'
        except (ValueError,TypeError,IndexError):http_date='UNKNOWN'
        review_documents(self.contracts['provider'])
        cap={'schema_version':VERSION,'kind':'UPSTREAM_RESPONSE','epoch_id':self.epoch['epoch_id'],
          'provider_identity':'TickFlow','endpoint':url.scheme+'://'+url.netloc+url.path,
          'sanitized_parameters':params,'request_fingerprint':digest(params),'security_universe':universe,
          'response_body_sha256':self.store.raw(body),'response_body_encoding':'HTTPX_DECODED_BODY_BYTES',
          'locally_fetched_at':fetched,'publisher_available_at':'UNKNOWN','provider_response_at':http_date,
          'provider_response_at_semantics':'HTTP_DATE_NOT_PUBLISHER_VINTAGE','HTTP_status':200,
          'adjustment_requested':'none','frequency':'1d','source_contract_identity':digest(self.contracts['provider']),
          'source_schema_version':'UNKNOWN','writer_attestation':self.attestation,'environment':self.epoch['environment']}
        cid=self.store.put(cap)
        for sym,values in data['data'].items():
            for i,stamp in enumerate(values.get('timestamp',[])):
                if any(k not in values or len(values[k])!=len(values['timestamp']) for k in FIELDS):continue
                day=datetime.fromtimestamp(stamp/1000,ZONE).date().isoformat()
                source={'symbol':sym,'timestamp':stamp,**{k:values[k][i] for k in FIELDS}}
                normalized=row({'symbol':sym,'date':day,**source})
                if day<=self.epoch['legacy_through'] or instant(fetched)<instant(self.epoch['activated_at']):continue
                h=digest(normalized)
                if len(self.capture_candidates)>=100000:self.capture_candidates.pop(next(iter(self.capture_candidates)))
                self.capture_candidates[h]={'capture':cid,'source_ordinal':i,'source_row':source,'source_row_sha256':digest(source),'normalized_row':normalized,'normalized_row_sha256':h}
    def bind(self,rows):
        for v in rows:
            n=row(v);h=digest(n);candidate=self.capture_candidates.get(h)
            if not candidate:continue
            obj={**candidate,'schema_version':VERSION,'kind':'NORMALIZED_ROW','epoch_id':self.epoch['epoch_id'],
                'normalization_code_identity':self.normalizer_identity,'normalized_at':now(),
                'transformations':{'column_mapping':{'trade_date':'date','vol':'volume','amt':'amount'},'date_conversion':'epoch milliseconds -> Asia/Shanghai date','numeric_cast':'Float64','halt_filter':'open/high zero rows excluded','symbol_normalization':'EXPLICIT_PROVIDER_SYMBOL; NO_PREFIX_GUESS','price_adjustment':'NONE','units_before_persistence':'LOCAL_REVIEWED_STOCK_REPOSITORY: CNY/LOTS/CNY'},
                'writer_attestation':self.attestation}
            self.pending[h]=self.store.put(obj)
            self.capture_candidates.pop(h,None)
    def publish(self,path,dataset,previous_hash,old_rows,new_rows,incoming_rows,persisted_at=None):
        if dataset!='kline_daily':return None
        day=path.parent.name.removeprefix('date=')
        if day<=self.epoch['legacy_through']:return None
        h=sha(path);old={r['symbol']:row(r) for r in old_rows};incoming={r['symbol']:row(r) for r in incoming_rows}
        prior={}
        if previous_hash:
            try:prior=self.store.publication(dataset,day,previous_hash)[1]['row_bindings']
            except (FileNotFoundError,ValueError,TypeError):pass
        bindings={};counts={'inserted':0,'replaced':0,'retained':0}
        for v in new_rows:
            n=row(v);symbol=n['symbol'];nh=digest(n)
            changed=symbol in incoming
            kind='inserted' if symbol not in old else 'replaced' if changed else 'retained';counts[kind]+=1
            proof=self.pending.get(nh) if changed else prior.get(symbol,{}).get('normalized_evidence')
            # Unknown legacy rows cannot be certified by surrounding rows.
            bindings[symbol]={'row_sha256':nh,'operation':kind,'normalized_evidence':proof,'provenance_status':'FORWARD_BOUND' if proof else 'LEGACY_OR_UNBOUND'}
        publication={'schema_version':VERSION,'kind':'PERSISTED_PUBLICATION','epoch_id':self.epoch['epoch_id'],'dataset':dataset,'partition_date':day,
          'pre_write_partition_sha256':previous_hash,'partition_sha256':h,'persisted_at':persisted_at or now(),
          'row_operations':counts,'row_bindings':bindings,'writer_attestation':self.attestation,'environment':self.epoch['environment']}
        pid=self.store.put(publication);self.store.index(dataset,day,h,pid)
        for v in incoming_rows:self.pending.pop(digest(row(v)),None)
        return pid


def verify_row(store,epoch,contracts,publication_id,symbol,returned,evaluated_at,reader_attestation,allow_test=False):
    """Verify every chain link. Returns metadata; exceptions mean OBSERVED, never QUALIFIED."""
    if epoch.get('schema_version')!=VERSION or epoch.get('active') is not True:raise ValueError('Forward epoch inactive')
    if epoch['environment']!='REAL_NORMAL_INGESTION' and not allow_test:raise ValueError('Fixture evidence not admitted in production')
    pub=store.get(publication_id);day=pub['partition_date']
    if day<=epoch['legacy_through'] or pub['epoch_id']!=epoch['epoch_id']:raise ValueError('Legacy/foreign epoch publication')
    binding=pub['row_bindings'].get(symbol)
    if not binding or not binding['normalized_evidence']:raise ValueError('Unbound legacy row')
    norm=store.get(binding['normalized_evidence']);cap=store.get(norm['capture'])
    if cap['epoch_id']!=epoch['epoch_id'] or norm['epoch_id']!=epoch['epoch_id'] or cap['environment']!=epoch['environment'] or pub['environment']!=epoch['environment']:raise ValueError('Cross-epoch evidence')
    data=json.loads(store.body(cap['response_body_sha256']))['data'];idx=norm['source_ordinal'];values=data[symbol]
    source={'symbol':symbol,'timestamp':values['timestamp'][idx],**{k:values[k][idx] for k in FIELDS}}
    normalized=row({'symbol':symbol,'date':datetime.fromtimestamp(source['timestamp']/1000,ZONE).date().isoformat(),**source})
    if source!=norm['source_row'] or digest(source)!=norm['source_row_sha256'] or normalized!=norm['normalized_row'] or digest(normalized)!=norm['normalized_row_sha256'] or binding['row_sha256']!=digest(normalized):raise ValueError('Response/source/normalized row binding drift')
    output={**normalized,'volume':normalized['volume']*100}
    if row(returned)!=output:raise ValueError('StockData returned row does not match bound publication')
    for aid,role in ((cap['writer_attestation'],'writer'),(pub['writer_attestation'],'writer'),(reader_attestation,'reader')):
        att=store.get(aid)
        if instant(att['started_at'])>instant(evaluated_at):raise ValueError('Attestation after evaluation')
        if att['epoch_id']!=epoch['epoch_id'] or att['role']!=role or att['actual_executed_code_identities']!=epoch['build']['roles'][role] or att['reviewed_build_id']!=digest(epoch['build']):raise ValueError('Loaded-build attestation missing/drift')
    if norm['normalization_code_identity']!=epoch['build']['roles']['writer']['normalize_daily']:raise ValueError('Normalizer revision mismatch')
    if cap['provider_identity']!='TickFlow' or cap['request_fingerprint']!=digest(cap['sanitized_parameters']):raise ValueError('Provider/request identity drift')
    if cap['HTTP_status']!=200 or cap['adjustment_requested']!='none' or cap['frequency']!='1d' or symbol not in cap['security_universe']:raise ValueError('Upstream request binding')
    contract=contracts['provider'];review_documents(contract)
    if cap['source_contract_identity']!=digest(contract) or contract['none_semantics']!='RAW_UNADJUSTED':raise ValueError('Unknown provider price basis')
    fetched=instant(cap['locally_fetched_at']);normalized_at=instant(norm['normalized_at']);persisted=instant(pub['persisted_at']);evaluated=instant(evaluated_at)
    if not instant(epoch['activated_at'])<=fetched<=normalized_at<=persisted<=evaluated:raise ValueError('Capture/normalization/publication/availability anti-lookahead')
    ex={'SH':'XSHG','SZ':'XSHE'}.get(symbol.rsplit('.',1)[-1]);close=session(contracts['calendar'],ex,day)
    if fetched<instant(close):raise ValueError('Same-session future/incomplete bar')
    available=max(fetched,normalized_at,persisted,instant(close)).isoformat()
    return {'forward_epoch':VERSION,'forward_epoch_id':epoch['epoch_id'],'available_at':available,'upstream_source':'TickFlow actual response; ultimate publisher UNKNOWN','provider_identity':'TickFlow',
      'price_vintage':{'kind':'VERIFIED_LOCAL_INGESTION_PUBLICATION_V1','available_at':available,'publisher_available_at':cap['publisher_available_at']},
      'upstream_version':'upstream-snapshot-sha256:'+digest({k:cap[k] for k in ('provider_identity','endpoint','request_fingerprint','response_body_sha256','locally_fetched_at','source_contract_identity')}),
      'upstream_response_sha256':cap['response_body_sha256'],
      'local_snapshot_version':'publication-sha256:'+publication_id,'calendar_version':'official-calendar-sha256:'+digest(contracts['calendar']),
      'server_loaded_version':'loaded-code-sha256:'+reader_attestation,'authoritative_calendar':'REVIEWED_EXCHANGE_DECLARED_SCOPE',
      'persisted_price_basis':'RAW_UNADJUSTED','provider_adjustment_semantics':contract['none_semantics'],
      'service_read_transform':'NO_PRICE_ADJUSTMENT; STOCK_VOLUME_LOTS_X100','effective_returned_basis':'RAW_UNADJUSTED','effective_price_basis':'raw',
      'bar_closure_verified':True,'bar_closure_semantics':'ORDINARY_DECLARED_COMPETITIVE_SESSION_COMPLETED_BEFORE_CAPTURE; NOT_SPECIAL_EVENT_ATTESTATION',
      'bar_end_at':close,'price_unit':'CNY','volume_unit':'shares','amount_unit':'CNY',
      'provenance':{'schema_version':VERSION,'publication':publication_id,'normalized_row':binding['normalized_evidence'],'response_capture':norm['capture'],'raw_response':cap['response_body_sha256'],'reader_attestation':reader_attestation,'calendar_contract':digest(contracts['calendar'])}}
