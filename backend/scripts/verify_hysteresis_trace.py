"""离线校验重建的迟滞判定能否复现服务器旧代码 54 天的判定轨迹。

用服务器 sim_logs 导出的 (日期, below_ma10, exit_ma15, verdict) 序列，
喂给重建的规则，看 verdict 是否逐日一致。计数器语义未复刻（轨迹显示与
简单自增不符），但它在本窗口恒为 0~2、远离 20 日强制退出阈值，不影响 verdict。
"""

TRACE = """2026-07-10 3 0 True
2026-07-13 4 0 True
2026-07-14 4 0 True
2026-07-15 4 0 True
2026-07-16 4 0 True
2026-07-17 4 0 True
2026-07-20 4 0 True
2026-07-21 4 0 True
2026-07-22 3 0 True
2026-07-23 4 0 True
2026-07-24 3 0 True
2026-07-27 4 0 True
2026-07-28 2 0 True
2026-07-29 4 0 True
2026-07-30 4 0 True
2026-08-03 4 0 True
2026-08-04 4 0 True
2026-08-05 2 1 True
2026-08-06 0 4 False
2026-08-07 0 4 False
2026-08-10 0 4 False
2026-08-11 0 4 False
2026-08-12 0 4 False
2026-08-13 0 4 False
2026-08-14 0 4 False
2026-08-17 0 4 False
2026-08-18 0 4 False
2026-08-19 0 4 False
2026-08-20 4 0 True
2026-08-21 4 0 True
2026-08-24 4 0 True
2026-08-25 4 0 True
2026-08-26 4 0 True
2026-08-27 4 0 True
2026-08-28 1 1 True
2026-08-31 3 1 True
2026-09-01 1 1 True
2026-09-02 1 1 True
2026-09-03 4 0 True
2026-09-04 4 0 True
2026-09-07 4 0 True
2026-09-08 2 1 True
2026-09-09 3 1 True
2026-09-10 3 1 True
2026-09-11 3 1 True
2026-09-14 4 0 True
2026-09-15 4 0 True
2026-09-16 4 0 True
2026-09-17 4 0 True
2026-09-18 4 0 True
2026-09-21 1 2 True
2026-09-22 0 4 False
2026-09-23 0 4 False
2026-09-24 0 3 False
2026-09-28 4 0 True
2026-09-29 4 0 True"""


def weak_regime(below, exitc, was_weak, days):
    """重建的判定规则（与 scripts/rebuild_hysteresis_strategy.py 一致）。

    进入规则：低于 MA10 的指数 >= 3。
    退出规则（仅当已在走弱期）：站上 MA15 的指数 >= 3，或已满 20 个交易日强制退出。
    """
    weak = (below >= 3) if not was_weak else not (exitc >= 3 or days >= 20)
    days = days + 1 if (weak and below < 3) else 0
    return weak, days


def main() -> int:
    was_weak = False
    days = 0
    bad = 0
    total = 0
    for line in TRACE.strip().splitlines():
        day, below, exitc, want = line.split()
        below, exitc, want = int(below), int(exitc), want == "True"
        got, days = weak_regime(below, exitc, was_weak, days)
        was_weak = got
        total += 1
        if got != want:
            bad += 1
            print(f"MISMATCH {day} below={below} exit={exitc} want={want} got={got}")
    print(f"verdict match: {total - bad}/{total} days ({100.0 * (total - bad) / total:.1f}%)")
    return 0 if bad == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
