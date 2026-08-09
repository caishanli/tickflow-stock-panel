"""quant 桥接层修复的单元测试（不跑 rqalpha 事件循环，直接测数据源/shim）。

覆盖：
- H1 盘中取数防前视：JqDataSource / QuantRQAlphaDataSource 的 history_bars '1d'
  切片侧别（盘中 <15:00 且 include_now=False 不含当日；15:00 起含当日）。
- H2 涨跌停价按昨收计算（日线/分钟 recarray；首日无前收为 NaN）。
- H3 基金类型判定（ETF/LOF/CS）与费用配置键名。
- H5 run_daily 时段外时刻归并（09:00→09:31、15:10→15:00）。
- M16 install_jqcompat 全量重置跨 run 全局状态。
- M9 get_all_securities 按 date 过滤上市/退市。
- 杂项：frequency 别名归一。
"""
import numpy as np
import pandas as pd
import pytest

from app.quant import jqcompat as jq
from app.quant import rqalpha_bridge as bridge


# ---------------------------------------------------------------------------
# 分钟数据覆盖告警：回测起点早于本地分钟数据首日时提示结果不可信
# ---------------------------------------------------------------------------

def test_minute_coverage_warning():
    import datetime as _dt
    warn = bridge._minute_coverage_warning
    assert warn("2026-04-01", None) is not None            # 无分钟数据 → 告警
    assert warn("2026-04-01", _dt.date(2026, 4, 1)) is None   # 等于覆盖首日 → 不告警
    assert warn("2026-06-01", _dt.date(2026, 4, 1)) is None   # 完全在覆盖内 → 不告警
    assert warn("2026-04-01", _dt.date(2026, 4, 2)) is not None  # 早于覆盖 → 告警


# ---------------------------------------------------------------------------
# 合成数据与 fake provider/DataManager
# ---------------------------------------------------------------------------
def _daily_df(closes=(10.0, 11.0, 12.0)):
    dates = ["2024-01-02", "2024-01-03", "2024-01-04"][: len(closes)]
    return pd.DataFrame({
        "date": pd.to_datetime(dates),
        "open": closes,
        "high": [c * 1.02 for c in closes],
        "low": [c * 0.98 for c in closes],
        "close": closes,
        "volume": [1000.0] * len(closes),
    })


class _FakeCache:
    def __init__(self, df):
        self._df = df

    def get_all(self, kind):
        if kind == "daily":
            return {"astock_510300.XSHG": self._df}
        return {}


class _FakeDM:
    """JqDataSource 需要的最小 DataManager 鸭子类型。"""

    def __init__(self, df, minute_df=None):
        self.cache = _FakeCache(df)
        self._df = df
        self._minute_df = minute_df

    def fetch(self, kind, code, start, end):
        return self._df

    def get_minute_feed(self, code, start, end):
        return self._minute_df


def _minute_df(day="2024-01-03"):
    idx = pd.to_datetime([f"{day} 09:31", f"{day} 09:32", f"{day} 10:00"])
    return pd.DataFrame({
        "open": [11.0, 11.1, 11.2],
        "high": [11.1, 11.2, 11.3],
        "low": [10.9, 11.0, 11.1],
        "close": [11.05, 11.15, 11.25],
        "volume": [100.0, 100.0, 100.0],
        "money": [1105.0, 1115.0, 1125.0],
    }, index=idx)


def _make_jq_ds(df=None, minute_df=None):
    df = df if df is not None else _daily_df()
    return jq.JqDataSource(_FakeDM(df, minute_df), ["510300.XSHG"],
                           "2024-01-02", "2024-01-04")


class _BundleLikeProvider:
    """QuantRQAlphaDataSource 需要的最小 provider（无 cache 属性 → 日历走标的日期并集）。"""

    def __init__(self, df):
        self._df = df

    def get_daily(self, code, start, end):
        return self._df


def _make_bridge_ds(df=None):
    df = df if df is not None else _daily_df()
    return bridge.QuantRQAlphaDataSource(
        _BundleLikeProvider(df), bridge.CONFIG,
        {"symbols": ["600000.XSHG"], "start": "2024-01-02", "end": "2024-01-04"})


# ---------------------------------------------------------------------------
# H1 盘中取数防前视
# ---------------------------------------------------------------------------
def _bar_days(bars):
    return (bars["datetime"] // 1000000).astype("int64").tolist()


def test_h1_jq_datasource_intraday_excludes_today_unless_include_now():
    ds = _make_jq_ds()
    ins = ds.get_instrument("510300.XSHG")
    # 盘中 10:00，include_now=False（默认）：不含当日（2024-01-04）bar
    bars = ds.history_bars(ins, 10, "1d", None, pd.Timestamp("2024-01-04 10:00"))
    assert _bar_days(bars) == [20240102, 20240103]
    # include_now=True：含当日 bar
    bars = ds.history_bars(ins, 10, "1d", None, pd.Timestamp("2024-01-04 10:00"),
                           include_now=True)
    assert _bar_days(bars) == [20240102, 20240103, 20240104]


def test_h1_jq_datasource_close_and_midnight_boundary():
    ds = _make_jq_ds()
    ins = ds.get_instrument("510300.XSHG")
    # 15:00（日线频率 handle_bar 触发时刻，当日日线已完整）：含当日 bar
    bars = ds.history_bars(ins, 10, "1d", None, pd.Timestamp("2024-01-04 15:00"))
    assert _bar_days(bars)[-1] == 20240104
    # 00:00（纯日期）：数据源层按 rqalpha 原生口径含当日——sys_analyser 以
    # dt=末日日期取基准日线依赖此语义；聚宽「不含当前 bar」由 get_price shim
    # 回退 end_dt 保证，不在数据源层排除
    bars = ds.history_bars(ins, 10, "1d", None, pd.Timestamp("2024-01-04 00:00"))
    assert _bar_days(bars)[-1] == 20240104
    # 盘中时刻（如 09:40）：当日日线未完成，不含当日 bar
    bars = ds.history_bars(ins, 10, "1d", None, pd.Timestamp("2024-01-04 09:40"))
    assert _bar_days(bars)[-1] == 20240103


def test_h1_jq_datasource_batch_same_semantics(monkeypatch):
    ds = _make_jq_ds()
    # history_bars_batch 经模块级 _instrument()（走 rqalpha Environment）解析标的，
    # 单元测试无 Environment，替换为数据源本地解析
    monkeypatch.setattr(jq, "_instrument",
                        lambda code: ds.get_instrument(code) if code == "510300.XSHG" else None)
    out = ds.history_bars_batch(["510300.XSHG"], 10, "1d", None,
                                pd.Timestamp("2024-01-04 10:00"))
    assert _bar_days(out["510300.XSHG"]) == [20240102, 20240103]
    out = ds.history_bars_batch(["510300.XSHG"], 10, "1d", None,
                                pd.Timestamp("2024-01-04 10:00"), include_now=True)
    assert _bar_days(out["510300.XSHG"])[-1] == 20240104


def test_h1_bridge_datasource_daily_handle_bar_keeps_today():
    """UI 路径（frequency=1d）：handle_bar 于 15:00 触发，当日 bar 可见（原生语义不误伤）。"""
    ds = _make_bridge_ds()
    ins = list(ds.get_instruments(["600000.XSHG"]))[0]
    # 日线 handle_bar（15:00）：含当日（datetime 为 rqalpha 口径 YYYYMMDD000000）
    bars = ds.history_bars(ins, 10, "1d", None, pd.Timestamp("2024-01-04 15:00"))
    assert _bar_days(bars)[-1] == 20240104
    # 盘中时刻（如 jqdata shim 在分钟回测盘中取日线）：不含当日
    bars = ds.history_bars(ins, 10, "1d", None, pd.Timestamp("2024-01-04 10:00"))
    assert _bar_days(bars) == [20240102, 20240103]
    # include_now=True：含当日
    bars = ds.history_bars(ins, 10, "1d", None, pd.Timestamp("2024-01-04 10:00"),
                           include_now=True)
    assert _bar_days(bars)[-1] == 20240104


def test_h1_bridge_datasource_midnight_dt_includes_today():
    """桥接源 dt=纯日期（00:00）按 rqalpha 原生口径含当日 bar。

    sys_analyser 以 dt=末日日期取基准日线，若排除当日，基准序列缺末日 bar，
    回测启动即报「基准标的可用行情数据…结束日期 <= 回测结束日期」（回归测试）。
    """
    ds = _make_bridge_ds()
    ins = list(ds.get_instruments(["600000.XSHG"]))[0]
    bars = ds.history_bars(ins, 10, "1d", None, pd.Timestamp("2024-01-04 00:00"))
    assert _bar_days(bars)[-1] == 20240104
    bars = ds.history_bars(ins, None, "1d", ["datetime", "close"],
                           pd.Timestamp("2024-01-04"))
    assert _bar_days(bars)[-1] == 20240104


# ---------------------------------------------------------------------------
# H2 涨跌停价按昨收计算
# ---------------------------------------------------------------------------
def test_h2_daily_recarray_limit_from_prev_close():
    arr = jq._daily_to_recarray(_daily_df(closes=(10.0, 11.0, 12.0)))
    # 首日无前收 → NaN（不用 open 估算）
    assert np.isnan(arr["limit_up"][0]) and np.isnan(arr["limit_down"][0])
    # 第 2/3 日按昨收 round(±10%, 2)
    assert arr["limit_up"][1] == pytest.approx(round(10.0 * 1.1, 2))
    assert arr["limit_down"][1] == pytest.approx(round(10.0 * 0.9, 2))
    assert arr["limit_up"][2] == pytest.approx(round(11.0 * 1.1, 2))
    assert arr["limit_down"][2] == pytest.approx(round(11.0 * 0.9, 2))


def test_h2_minute_recarray_limit_from_prev_close_map():
    arr = jq._minute_to_recarray(
        _minute_df("2024-01-03"), prev_close_map={20240103: 10.0})
    assert (arr["limit_up"] == pytest.approx(round(10.0 * 1.1, 2)))
    assert (arr["limit_down"] == pytest.approx(round(10.0 * 0.9, 2)))
    # 无昨收映射 → NaN
    arr2 = jq._minute_to_recarray(_minute_df("2024-01-03"))
    assert np.isnan(arr2["limit_up"]).all()


def test_h2_jq_datasource_minute_limit_wired_from_daily():
    """JqDataSource._ensure_minute 应从日线昨收接线分钟涨跌停（_prev_close 不再只写不读）。"""
    ds = _make_jq_ds(df=_daily_df(closes=(10.0, 11.0, 12.0)),
                     minute_df=_minute_df("2024-01-03"))
    arr = ds._ensure_minute("510300.XSHG")
    # 2024-01-03 的昨收 = 2024-01-02 收盘 10.0
    assert set(np.unique(arr["limit_up"])) == {round(10.0 * 1.1, 2)}
    assert ds._prev_close["510300.XSHG"][20240103] == 10.0
    assert ds._prev_close["510300.XSHG"][20240104] == 11.0


def test_h2_bridge_daily_recarray_limit_from_prev_close():
    arr = bridge.QuantRQAlphaDataSource._df_to_recarray(_daily_df(closes=(10.0, 11.0, 12.0)))
    assert np.isnan(arr["limit_up"][0])
    assert arr["limit_up"][1] == pytest.approx(round(10.0 * 1.1, 2))
    assert arr["limit_down"][2] == pytest.approx(round(11.0 * 0.9, 2))


# ---------------------------------------------------------------------------
# H3 基金类型判定 + 费用键名
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("code,expected", [
    ("510300.XSHG", "ETF"), ("561800.XSHG", "ETF"), ("588000.XSHG", "ETF"),
    ("501018.XSHG", "LOF"),
    ("159915.XSHE", "ETF"), ("160416.XSHE", "LOF"),
    ("600000.XSHG", "CS"), ("000001.XSHE", "CS"), ("000300.XSHG", "CS"),
])
def test_h3_fund_instrument_type(code, expected):
    assert jq._fund_instrument_type(code) == expected
    assert bridge._fund_instrument_type(code) == expected


def test_h3_jq_instrument_type_and_shared_store():
    ds = _make_jq_ds()
    ins = ds.get_instrument("510300.XSHG")
    assert ins.type == jq.INSTRUMENT_TYPE.ETF
    # ETF 类型标的的日线 store 已注册（按 instrument.type 查得到 bar）
    bars = ds._all_bars_of(ins, "1d")
    assert len(bars) == 3


def test_h3_bridge_instrument_type_and_shared_store():
    ds = _make_bridge_ds()
    ins = list(ds.get_instruments(["600000.XSHG"]))[0]
    assert ins.type == bridge.INSTRUMENT_TYPE.CS
    df = _daily_df()
    ds2 = bridge.QuantRQAlphaDataSource(
        _BundleLikeProvider(df), bridge.CONFIG,
        {"symbols": ["510300.XSHG"], "start": "2024-01-02", "end": "2024-01-04"})
    ins2 = list(ds2.get_instruments(["510300.XSHG"]))[0]
    assert ins2.type == bridge.INSTRUMENT_TYPE.ETF
    assert len(ds2._all_day_bars_of(ins2)) == 3


def test_h3_build_config_fee_keys():
    cfg = bridge._build_config({"start": "2024-01-02", "end": "2024-01-04",
                                "fee": 0.001, "min_commission": 0})
    cost = cfg["mod"]["sys_transaction_cost"]
    assert set(cost.keys()) == {"stock_commission_multiplier", "stock_min_commission"}
    assert cost["stock_commission_multiplier"] == pytest.approx(0.001 / 0.0008)
    assert cost["stock_min_commission"] == 0.0


# ---------------------------------------------------------------------------
# H5 run_daily 时段外时刻归并
# ---------------------------------------------------------------------------
def test_h5_run_daily_clamps_out_of_session_times():
    jq.install_jqcompat([])
    try:
        jq.run_daily(lambda c: None, time="09:00")
        jq.run_daily(lambda c: None, time="15:10")
        jq.run_daily(lambda c: None, time="10:30")
        jq.run_daily(lambda c: None, time="before_trading")
        jq.run_daily(lambda c: None, time="close")
        assert (9, 31) in jq._DAILY_AT      # 09:00 与 before_trading 归并到首根 bar
        assert len(jq._DAILY_AT[(9, 31)]) == 2
        assert (15, 0) in jq._DAILY_AT       # 15:10 与 close 归并到末根 bar
        assert len(jq._DAILY_AT[(15, 0)]) == 2
        assert (10, 30) in jq._DAILY_AT      # 交易时段内时刻保持原样
        assert (9, 0) not in jq._DAILY_AT and (15, 10) not in jq._DAILY_AT
    finally:
        jq.install_jqcompat([])  # 复位，避免污染同进程其他测试


# ---------------------------------------------------------------------------
# M16 install_jqcompat 全量重置
# ---------------------------------------------------------------------------
def test_m16_install_resets_global_state():
    jq.install_jqcompat(["510300.XSHG"])
    jq.run_daily(lambda c: None, time="10:00")
    jq.run_daily(lambda c: None, time="every_bar")
    jq._set_current_bar_dict(object())
    assert jq._DAILY_AT and jq._EVERY_BAR_CALLBACKS and jq._CURRENT_BAR_DICT is not None
    # 连续第二次 install：上一次回测的回调/快照必须清空
    jq.install_jqcompat(["159915.XSHE"])
    assert jq._EVERY_BAR_CALLBACKS == []
    assert jq._DAILY_AT == {}
    assert jq._CURRENT_BAR_DICT is None
    assert jq._UNIVERSE == ["159915.XSHE"]


# ---------------------------------------------------------------------------
# M9 get_all_securities 按 date 过滤
# ---------------------------------------------------------------------------
def test_m9_get_all_securities_filters_by_date():
    jq.install_jqcompat(
        ["510300.XSHG", "513500.XSHG", "159001.XSHE"],
        list_dates={
            "513500.XSHG": ("2023-06-01", "2999-12-31"),   # 2023 年才上市
            "159001.XSHE": ("2015-01-01", "2022-12-31"),   # 已退市
            # 510300.XSHG 无数据 → 退化为全程可交易（保持修复前行为）
        })
    try:
        df = jq.get_all_securities(date="2020-01-01")
        assert set(df.index) == {"510300.XSHG", "159001.XSHE"}
        df = jq.get_all_securities(date="2024-01-01")
        assert set(df.index) == {"510300.XSHG", "513500.XSHG"}
        # 不传 date：全量返回，start_date 如实反映 list_date
        df = jq.get_all_securities()
        assert set(df.index) == {"510300.XSHG", "513500.XSHG", "159001.XSHE"}
        assert df.loc["513500.XSHG", "start_date"] == "2023-06-01"
    finally:
        jq.install_jqcompat([])


# ---------------------------------------------------------------------------
# 杂项：frequency 别名归一 + _build_config 生效
# ---------------------------------------------------------------------------
def test_misc_frequency_param_normalized():
    assert bridge._norm_frequency("daily") == "1d"
    assert bridge._norm_frequency("1m") == "1m"
    assert bridge._norm_frequency("minute") == "1m"
    assert bridge._norm_frequency(None) == "1d"
    cfg = bridge._build_config({"start": "2024-01-02", "end": "2024-01-04",
                                "frequency": "1m"})
    assert cfg["base"]["frequency"] == "1m"
    cfg = bridge._build_config({"start": "2024-01-02", "end": "2024-01-04",
                                "frequency": "daily"})
    assert cfg["base"]["frequency"] == "1d"
    # UI 路径也启用 jqbarcache 调度（run_daily 需要）
    assert cfg["mod"]["jqbarcache"] == {"enabled": True}


def test_fallback_price_uses_string_code_and_attribute_access(monkeypatch):
    """_fallback_price 对 BarObject 必须用属性访问（b.close 而非 b["close"]），
    且 data_proxy.get_bar 必须传 order_book_id 字符串（rqalpha 6.x 契约）。

    回归：此前传 Instrument 对象 + b["close"]（BarObject.__getitem__ 读实例
    __dict__，cached_property 未访问前不存在 → KeyError）被 except 吞掉，
    未订阅标的 last_price 恒为 0——动量分被拉爆、low_limit=0 触发"跌停跳过买入"。
    """
    import types as _t

    calls = []

    class _FakeDP:
        def get_bar(self, code, dt, freq):
            calls.append((code, freq))
            ins = ds.get_instrument(code)
            return dp_bar(ins)

    from rqalpha.data.data_proxy import DataProxy
    from rqalpha.model.bar import BarObject

    ds = _make_jq_ds()
    raw = ds.get_bar(ds.get_instrument("510300.XSHG"),
                     __import__("pandas").Timestamp("2024-01-03"), "1d")

    def dp_bar(ins):
        return BarObject(ins, raw)

    env = _t.SimpleNamespace(data_proxy=_FakeDP(), trading_dt="2024-01-03 13:10")
    sd = jq._SecurityData("510300.XSHG")
    # _SecurityData._env 是只读 property（内部调 Environment.get_instance），
    # 直接 patch get_instance 即可；_instrument 依赖 rqalpha Environment，单测
    # 环境不存在，替换为数据源本地解析（与 test_h1 batch 用例同手法）
    monkeypatch.setattr(jq.Environment, "get_instance", classmethod(lambda cls: env))
    monkeypatch.setattr(jq, "_instrument",
                        lambda code: ds.get_instrument(code) if code == "510300.XSHG" else None)

    price = sd._fallback_price()
    # '1m' 先尝试，FakeDP 对 '1m' 也返回同一 bar（验证传参是字符串、属性访问可用）
    assert calls and isinstance(calls[0][0], str) and calls[0][0] == "510300.XSHG"
    assert price == float(raw["close"]) > 0
    # 涨跌停同样用属性访问
    assert sd.high_limit == float(raw["limit_up"])
    assert sd.low_limit == float(raw["limit_down"])
