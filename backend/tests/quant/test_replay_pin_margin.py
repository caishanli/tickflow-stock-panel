"""补跑钉窗必须给起点留分钟回看余量（首板质量读昨日分钟）。

回归：2026-10-09 sb_gaokai_sim 从 10-08 倒带补跑时，钉窗 [10-08, 10-09]
把 09-30 裁掉，10-08 当天全部候选刷"无昨日分钟数据"，603906 错过。
"""
import datetime

from app.quant.simulate import runner


class _FakeDM:
    def __init__(self):
        self.calls = []

    def set_minute_window(self, s, e):
        self.calls.append((s, e))


def test_pin_extends_start_by_lookback_margin():
    dm = _FakeDM()
    runner._pin_replay_minute_window(dm, None, "2026-10-08", datetime.date(2026, 10, 9))
    assert dm.calls == [("2026-09-23", "2026-10-09")]
