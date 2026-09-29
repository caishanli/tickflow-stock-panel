"""从 v5.6.2 无状态版反推重建 05d2a7d 之前的迟滞版走弱期判定。

用途：量化"带迟滞的旧判定"与"无状态新判定"在本窗口的收益差，
复现服务器 93e6e796 / e3f59fc0 那两个旧代码账户的 +17.49% / +29.28%。

**这是重建品，不是原件**——原 61c82080.py 于 2026-09-22 被覆盖，磁盘无备份、
data/ 不在 git、回测 params_json 不内联源码、进程内存里源码已被 compile() 释放。

重建依据（服务器 sim_logs 里旧代码 54 天的判定轨迹，verdict 列）：
  - 进入弱期：below_ma10 >= 3
  - 退出弱期：站上 MA15 的指数数 >= 3（退出确认）
  - 20 个交易日未退出则强制退出（force-exit）
轨迹中 weak_days_count 恒在 0~2，远离 20，本窗口内该分支不触发，
故计数器语义（轨迹显示与简单自增不符）不影响判定结果，未复刻。

日志格式对齐旧代码，便于与服务器 sim_logs 直接对拍。
"""
import sys

SRC = "../data/quant_strategies/dual_v55_opt.py"
DST = "../data/quant_strategies/dual_v55_opt_hysteresis.py"

NEW_FN_START = "def _compute_weak_regime():"
NEW_FN_END = "def apply_filters(metrics_list):"

OLD_FN = '''def _compute_weak_regime():
    """走弱期判定（重建的迟滞版，对应 05d2a7d 之前的行为）。

    与 v5.6.2 无状态版的差别只有"退出"：
      - 无状态版：below_ma10 >= 3 即走弱，否则正常（单日可翻转）
      - 迟滞版：进入后需站上 MA15 的指数 >= 3 才退出，或满 20 个交易日强制退出

    因此 2/4 这种"不够弱但也没站上 MA15"的日子，无状态版判正常期（切到
    178 只合并池），迟滞版仍维持走弱期（沿用 16 只全球/海外池）——这正是
    07-28 两机判定分叉、进而选股分叉的机制。
    """
    indexes = {
        '大盘': '000300.XSHG',
        '小盘': '399101.XSHE',
        '创业板': '399006.XSHE',
        '中证A500': '000510.XSHG'
    }
    ma_n = g.weak_period_ma_lookback
    below_count = 0
    exit_count = 0
    assessed = 0
    for name, code in indexes.items():
        try:
            df = attribute_history(code, 16, '1d', ['close'], skip_paused=False)
        except Exception as e:
            log.warning(f"📊 【走弱期判断】{name}({code})取数异常: {e}")
            continue
        if df is None or len(df) < ma_n:
            log.warning(f"📊 【走弱期判断】{name}({code})数据不足，跳过该指数")
            continue
        assessed += 1
        current_price = df['close'][-1]
        ma_val = df['close'][-ma_n:].mean()
        is_below = current_price < ma_val
        below_count += 1 if is_below else 0
        status_emoji = "⬇️低于" if is_below else "⬆️站上"
        log.info(f"📊 【走弱期判断】{name}({code}): 收盘{current_price:.2f} / MA{ma_n} {ma_val:.2f} → {status_emoji}")
        ma15 = df['close'][-15:].mean()
        if current_price > ma15:
            exit_count += 1
    if assessed < len(indexes):
        log.warning(f"📊 【走弱期判断】仅{assessed}/{len(indexes)}只指数可用，沿用当前判定: 走弱期={getattr(g, 'is_a_share_weak', False)}")
        return getattr(g, 'is_a_share_weak', False)

    was_weak = bool(getattr(g, 'is_a_share_weak', False))
    days = int(getattr(g, 'weak_days_count', 0) or 0)
    forced = False
    if not was_weak:
        weak = below_count >= 3
    else:
        if exit_count >= 3:
            weak = False
        elif days >= 20:
            weak = False
            forced = True
        else:
            weak = True
    # 计数：走弱且未达 3/4 时自增；重新满足进入条件则归零（显示用）
    if weak and below_count < 3:
        days += 1
    else:
        days = 0
    g.weak_days_count = days
    log.info(f"📊 【走弱期判断】低于MA{ma_n}: {below_count}/4, 站上MA15(退出): {exit_count}/4")
    suffix = " (20日强制退出)" if forced else ""
    log.info(f"📊 【走弱期判断】🔴 最终状态: 走弱期={weak} (已持续{days}/20个交易日){suffix}")
    return weak


'''


def main() -> int:
    with open(SRC, encoding="utf-8") as fh:
        src = fh.read()
    i = src.index(NEW_FN_START)
    j = src.index(NEW_FN_END)
    out = src[:i] + OLD_FN + src[j:]

    # 标记来源，避免与原版混淆
    banner = (
        "# ============================================================================\n"
        "# 【重建品·勿用于实盘】dual_v55_opt 的迟滞版走弱期判定（05d2a7d 之前行为）\n"
        "# 原件已不可得（文件被覆盖 / data/ 不在 git / 回测不内联源码 / 内存已释放）。\n"
        "# 唯一改动：_compute_weak_regime 换回带迟滞的版本，其余逻辑与现版完全一致。\n"
        "# ============================================================================\n"
    )
    out = banner + out

    # 旧版依赖 g.weak_days_count 跨日状态：init 里初始化
    out = out.replace(
        "    g.weak_period_ma_lookback = 10\n    g.is_a_share_weak = _compute_weak_regime()",
        "    g.weak_period_ma_lookback = 10\n"
        "    g.weak_days_count = 0  # 迟滞版跨日状态（旧实现存进程内存，重启即丢）\n"
        "    g.is_a_share_weak = _compute_weak_regime()",
    )

    with open(DST, "w", encoding="utf-8") as fh:
        fh.write(out)
    print("written:", DST, len(out), "chars")
    # 语法自检
    compile(out, DST, "exec")
    print("syntax OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
