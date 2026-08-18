"""ptrade 双持仓策略文件验证：编译 + API 清单（无聚宽独有调用）+ 双持仓配置。"""
import py_compile
import re
from pathlib import Path

STRATEGY = Path(__file__).parent.parent / "fixtures" / "dual_v54" / "wufu-v5.4-dual-adapt.ptrade.py"
_JQ_ONLY = ["jqdata", "get_current_data", "attribute_history", "get_price(", "record(",
            "set_option", "set_order_cost", "get_all_securities(", "get_security_info",
            "run_daily(morning_routine", "every_bar"]


def test_compiles():
    assert STRATEGY.exists()
    py_compile.compile(str(STRATEGY), doraise=True)


def _code_lines():
    """剔除注释与空行，保留可执行代码行。"""
    out = []
    for line in STRATEGY.read_text(encoding="utf-8").splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        out.append(line)
    return out


def test_no_jq_apis():
    for line in _code_lines():
        for kw in _JQ_ONLY:
            assert kw not in line, kw


def test_dual_position_config():
    src = STRATEGY.read_text(encoding="utf-8")
    assert "g.holdings_num = 2" in src
    assert "cross_slot1_floor" in src
    assert "cross_adaptive" in src
    assert "g.target_weights = [0.5, 0.5]" in src
    assert "select_cross_asset_dual" in src
    assert "run_daily(context, afternoon_routine, time='13:10')" in src
    assert "run_daily(context, sell_routine, time='13:10')" in src
    assert "run_daily(context, buy_routine, time='13:10')" in src


def test_no_fstring_log():
    """PTrade 日志用 % 格式化，不引入 f-string（\bf['"] 只匹配字符串前缀 f）。"""
    for line in _code_lines():
        assert not re.search(r"\bf([\"'])", line), f"策略不应使用 f-string: {line}"


def test_no_log_warn():
    """PTrade LogEngine 无 warn 方法（只有 warning），不得使用 log.warn。"""
    src = STRATEGY.read_text(encoding="utf-8")
    assert "log.warn(" not in src
    assert "log.warning(" in src


def test_capture_bars_guards_non_dict_data():
    """before_trading_start 的 data 是 StrategyUniverse（无 __len__/无行情），_capture_bars 必须跳过而非 or {}。"""
    src = STRATEGY.read_text(encoding="utf-8")
    assert "data.items()" in src
    assert "data or {}" not in src
    assert "no __len__" in src or "StrategyUniverse" in src


_CONV_LAYER = ["_pt(", "_cd(", "_cd_field", "_set_last_data", "_BarUnit", "_safe_log",
               "_warn(", "_debug(", "_as_series_values", "_positions_map",
               "_get_position(", "_pos_amount", "_pos_avail", "_pos_cost", "_pos_price",
               "_get_total_value", "_get_available_cash", "_update_universe", "_current_price(",
               "_get_today_volume("]


def test_no_conversion_layer():
    """jq→ptrade 转换层已删除：可执行代码不得出现任何转换函数调用。"""
    for line in _code_lines():
        for kw in _CONV_LAYER:
            assert kw not in line, kw
