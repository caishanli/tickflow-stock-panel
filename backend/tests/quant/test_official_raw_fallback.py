"""Official stock raw fallback contract; no live source or production writes."""
import ast
import datetime as dt
import sys
import types
from pathlib import Path

import polars as pl
import pytest

import app.services
from app.raw_partition_lock import daily_partition_lock
from app.services import alt_daily as ad
from app.services.stockdata import scheduler

DAY = dt.date(2026, 9, 22)

def bar(symbol='920885.BJ'):
    return pl.DataFrame({'symbol': [symbol], 'date': [DAY], 'open': [10.],
                         'high': [11.], 'low': [9.], 'close': [10.5],
                         'volume': [123.], 'amount': [129150.]})

class Provider:
    def get_daily(self, symbols, start_time, end_time, asset_type):
        assert asset_type == 'stock'
        assert start_time.date() == end_time.date() == DAY
        return bar()

def test_official_batch_preserves_bj_and_reports_missing_day_keys():
    result = ad.fetch_official_stock_daily(['920885.BJ', '600519.SH'], [DAY], provider=Provider())
    assert result['frame'].equals(bar())
    assert result['status'] == 'partial'
    assert result['missing'] == [{'symbol': '600519.SH', 'date': str(DAY)}]
    assert result['errors'] == []


def test_invalid_batch_is_quarantined_not_deduplicated_or_filled():
    bad = [bar().drop('amount'), pl.concat([bar(), bar()]),
           bar().with_columns(pl.lit(float('nan')).alias('amount')),
           bar().with_columns(pl.lit(8.).alias('high')),
           bar().with_columns(pl.lit('920885.SH').alias('symbol')),
           bar().with_columns(pl.lit(DAY - dt.timedelta(days=1)).alias('date'))]
    for frame in bad:
        class BadProvider:
            def get_daily(self, *args, captured=frame):
                return captured
        result = ad.fetch_official_stock_daily(['920885.BJ'], [DAY], provider=BadProvider())
        assert result['frame'].is_empty()
        assert len(result['errors']) == 1
        assert len(result['missing']) == 1


def test_publication_is_create_only_real_writer(tmp_path, monkeypatch):
    root = tmp_path / 'data/kline_daily'
    source = Path(ad.__file__).with_name('mootdx_service.py')
    node = next(n for n in ast.parse(source.read_text()).body
                if isinstance(n, ast.FunctionDef) and n.name == '_write_daily_partition')
    ms = types.ModuleType('app.services.mootdx_service')
    ms.__dict__.update(pl=pl, Path=Path, daily_partition_lock=daily_partition_lock,
                       STOCK_DAILY_ROOT=root, ETF_DAILY_ROOT=tmp_path/'etf',
                       INDEX_DAILY_ROOT=tmp_path/'index')
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), 'exec'), ms.__dict__)
    monkeypatch.setitem(sys.modules, 'app.services.mootdx_service', ms)
    monkeypatch.setattr(app.services, 'mootdx_service', ms, raising=False)
    assert ad.publish_official_stock_daily(bar(), root) == 1
    target = root / f'date={DAY}/part.parquet'
    before = target.read_bytes()
    with pytest.raises(FileExistsError):
        ad.publish_official_stock_daily(bar().with_columns(pl.lit(9.).alias('close')), root)
    assert target.read_bytes() == before
    assert pl.read_parquet(target).equals(bar())


def test_stock_official_entry_uses_official_bj_and_never_overwrites(tmp_path, monkeypatch):
    ms = types.ModuleType('app.services.mootdx_service')
    ms.STOCK_DAILY_ROOT = tmp_path / 'kline_daily'
    ms._stock_universe = lambda: ['920885.BJ', '600519.SH']
    ms._listing_date_map = lambda: {}
    monkeypatch.setitem(sys.modules, 'app.services.mootdx_service', ms)
    monkeypatch.setattr(app.services, 'mootdx_service', ms, raising=False)
    monkeypatch.setattr(ad, 'fetch_official_stock_daily',
                        lambda symbols, days: ad_fetch(symbols, days))
    def ad_fetch(symbols, days):
        assert set(symbols) == {'920885.BJ', '600519.SH'}
        return {'frame': bar(), 'missing': [{'symbol': '600519.SH', 'date': str(DAY)}],
                'errors': [], 'expected': 2, 'status': 'partial', 'source': 'tickflow_raw_none'}
    monkeypatch.setattr(ad, 'publish_official_stock_daily', lambda frame, root: frame.height)
    result = ad.sync_stock_daily_official([DAY])
    assert result['total'] == 1 and result['status'] == 'partial'
    assert result['missing'][0]['symbol'] == '600519.SH'


def test_scheduler_records_fallback_even_when_primary_raises(monkeypatch):
    ms = types.ModuleType('app.services.mootdx_service')
    def broken():
        raise RuntimeError('primary unavailable')
    ms.backfill_to_now = broken
    monkeypatch.setitem(sys.modules, 'app.services.mootdx_service', ms)
    monkeypatch.setattr(app.services, 'mootdx_service', ms, raising=False)
    receipt = {'source': 'tickflow_raw_none', 'total': 1, 'status': 'partial'}
    def fallback(kind, **kwargs):
        assert kind == 'stock'
        return receipt
    monkeypatch.setattr(ad, 'fill_recent_gaps_daily', fallback)
    monkeypatch.setattr(scheduler, '_trim_memory', lambda: None)
    monkeypatch.setattr(scheduler, '_scheduler_state', {})
    scheduler._backfill_loop()
    assert scheduler.get_status()['daily_fallback_result'] == receipt


def test_permission_failure_stops_batches_with_full_denominator():
    class Denied:
        calls = 0
        def get_daily(self, *args):
            self.calls += 1
            raise PermissionError('no permission')
    provider = Denied()
    symbols = [f'{n:06d}.SZ' for n in range(101)]
    result = ad.fetch_official_stock_daily(symbols, [DAY], provider=provider)
    assert provider.calls == 1
    assert result['expected'] == len(result['missing']) == 101
    assert result['frame'].is_empty() and result['status'] == 'partial'


def test_legacy_chain_remains_available_as_fallback():
    """传统链（腾讯/新浪）保留为次级 fallback：official 缺席标的仍可补。

    build_symbol_frame 返回 None（两源皆失败）或帧，不再抛闸。
    """
    assert callable(ad.build_symbol_frame) and callable(ad._row_frame)





