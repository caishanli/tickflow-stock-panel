"""wufu-v5.4-ding 策略文件验证：语法 + 买卖路径含 log.notify。"""
import py_compile
from pathlib import Path

STRATEGY = Path(__file__).parent.parent / "fixtures" / "wufu_v54" / "wufu-v5.4-ding.py"


def test_strategy_compiles():
    assert STRATEGY.exists()
    py_compile.compile(str(STRATEGY), doraise=True)


def test_strategy_has_notify_on_buy_and_sell():
    src = STRATEGY.read_text(encoding="utf-8")
    assert "log.notify" in src
    assert "📥 买入" in src
    assert "📤 卖出" in src
    assert "g._entry_date" in src
    assert "_notify_trade" in src
