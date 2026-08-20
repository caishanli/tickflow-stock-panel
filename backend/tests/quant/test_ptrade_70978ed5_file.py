"""70978ed5 ptrade 原生版文件验证：编译 + 无聚宽独有调用 + 调度断言。"""
import py_compile
from pathlib import Path

PT_FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "70978ed5_ptrade"
PT_STRATEGY = PT_FIXTURE_DIR / "70978ed5.ptrade.py"
JQ_SOURCE = PT_FIXTURE_DIR / "70978ed5.py"

_JQ_ONLY = [
    "jqdata", "get_current_data", "attribute_history", "record(",
    "set_option", "set_order_cost", "get_all_securities", "get_security_info",
    "get_security_name(", "every_bar",
]

_CONV_LAYER = [
    "_pt(", "_cd(", "_cd_field", "_set_last_data", "_BarUnit", "_safe_log",
    "_warn(", "_debug(", "_as_series_values", "_positions_map",
    "_get_position(", "_pos_amount", "_pos_avail", "_pos_cost", "_pos_price",
    "_get_total_value", "_get_available_cash", "_update_universe", "_current_price(",
    "_get_today_volume(",
]


def _code_lines():
    out = []
    for line in PT_STRATEGY.read_text(encoding="utf-8").splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        out.append(line)
    return out


def test_jq_source_exists():
    assert JQ_SOURCE.exists()
    assert "from jqdata import *" in JQ_SOURCE.read_text(encoding="utf-8")

def test_ptrade_compiles():
    assert PT_STRATEGY.exists()
    py_compile.compile(str(PT_STRATEGY), doraise=True)

def test_no_jq_apis():
    for line in _code_lines():
        for kw in _JQ_ONLY:
            assert kw not in line, kw

def test_no_conversion_layer():
    for line in _code_lines():
        for kw in _CONV_LAYER:
            assert kw not in line, kw

def test_no_xshg_codes_in_code():
    for line in _code_lines():
        assert ".XSHG" not in line and ".XSHE" not in line, line

def test_uses_docx_apis():
    src = PT_STRATEGY.read_text(encoding="utf-8")
    for api in ["get_history", "get_price", "get_stock_status", "get_stock_name",
                "get_etf_list", "get_market_list", "get_market_detail",
                "run_daily(context", "before_trading_start", "after_trading_end",
                "handle_data", "context.blotter.current_dt", "context.previous_date"]:
        assert api in src, api

def test_run_daily_count_within_limit():
    import re
    src = PT_STRATEGY.read_text(encoding="utf-8")
    count = len(re.findall(r"run_daily\s*\(", src))
    assert 1 <= count <= 5, f"run_daily 总数 {count} 超出 PTrade 限制 5"

def test_thirteen_ten_tasks_same_time():
    src = PT_STRATEGY.read_text(encoding="utf-8")
    assert src.count("time='13:10'") == 3, "13:10 同点任务应为 3 个"
    assert src.count("time='09:40'") == 1
    for fn in ["afternoon_routine", "sell_routine", "buy_routine"]:
        assert f"run_daily(context, {fn}, time='13:10')" in src, fn

def test_no_fstring_log():
    import re
    for line in _code_lines():
        assert not re.search(r"\bf([\"'])", line), f"策略不应使用 f-string: {line}"

def test_no_log_warn():
    src = PT_STRATEGY.read_text(encoding="utf-8")
    assert "log.warn(" not in src
