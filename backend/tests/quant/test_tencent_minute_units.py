"""腾讯 m1 成交量单位的板块差异（科创板不换算）。

背景：腾讯 mkline 对主板/创业板/ETF 返回「手」，但对科创板 688/689 直接
返回「股」。旧解析器一律 ×100，导致 2026-09-10 起 ``kline_minute`` 里科创板
分钟量/额全部 100 倍虚高。
"""

from datetime import date

from app.services.tencent_minute import parse_m1_rows

ROW = ["202611061500", "35.69", "36.68", "36.68", "35.69", "95429.00", {}, "2.02"]


def test_star_minute_volume_kept_as_shares():
    df = parse_m1_rows("688001.SH", [ROW], {date(2026, 11, 6)})
    assert df["volume"][0] == 95429.0          # 不能再 ×100


def test_main_board_minute_volume_converted_to_shares():
    df = parse_m1_rows("600000.SH", [ROW], {date(2026, 11, 6)})
    assert df["volume"][0] == 9_542_900.0
