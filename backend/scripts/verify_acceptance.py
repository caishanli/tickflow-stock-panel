#!/usr/bin/env python3
"""多源数据核验的三项验收检查。

默认（及显式 ``--dry-run``）只读：科创板项的「修复数」表示可修复数；
只有显式 ``--apply`` 才会调用 runner 修复并复查。qfq 项把 baostock 放在
独立子进程中，硬超时后终止，避免其 socket 挂起拖死验收脚本。

科创板检查区分两种 PASS：修复前的「检出且可修复」（``detected >= 600``）
与修复完成后的「clean」（``detected == 0``）。部分检出或仍有残留一律 FAIL。
"""
from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.quant.data_verify.checks_internal import (  # noqa: E402
    MINUTE_CLOSE_MISMATCH,
    MINUTE_VOLUME_MISMATCH,
    MINUTE_VOLUME_X100,
    compare_minute_to_daily,
)
from app.quant.data_verify.findings import SEVERITY_SOFT  # noqa: E402

STAR_PREFIXES = ("688", "689")
QFQ_START = "2023-04-19"
QFQ_END = "2023-12-20"
SPECIAL_DAYS = ("2016-01-04", "2018-06-01")
_CHILD_MARKER = "__VERIFY_ACCEPTANCE_JSON__"


def _result(status: str, detail: str, **values: Any) -> dict[str, Any]:
    return {"status": status, "detail": detail, **values}


def _part(data_root: Path, table: str, day: str) -> Path:
    return data_root / table / f"date={day}" / "part.parquet"


def _star_findings(data_root: Path, day: str):
    minute = pl.read_parquet(_part(data_root, "kline_minute", day))
    daily = pl.read_parquet(_part(data_root, "kline_daily", day))
    findings = compare_minute_to_daily(minute, daily, table="kline_minute")
    return [
        finding for finding in findings
        if finding.symbol and finding.symbol.startswith(STAR_PREFIXES)
    ]


def check_star_x100(
    data_root: Path,
    days: list[str],
    *,
    min_per_day: int = 600,
    apply: bool = False,
) -> dict[str, Any]:
    """验收科创板 ×100：修复前检出/修复不少于 600，或修复后已 clean。

    每天独立判断：``detected >= min_per_day`` 且无残留为 repairable；
    ``detected == 0`` 且无残留为 clean。两者都是 PASS；部分检出
    （``0 < detected < min_per_day``）或仍有残留保持 FAIL。
    """
    per_day: list[dict[str, Any]] = []
    ok = True
    for day in days:
        if not all(_part(data_root, table, day).is_file()
                   for table in ("kline_minute", "kline_daily")):
            per_day.append({"day": day, "error": "缺少分钟或日线分区",
                            "state": "invalid", "ok": False})
            ok = False
            continue
        before = [f for f in _star_findings(data_root, day) if f.code == MINUTE_VOLUME_X100]
        detected = len(before)
        if apply and detected:
            from app.quant.data_verify.runner import run_verify

            run_verify(
                "internal", apply=True, days=[date.fromisoformat(day)], data_root=data_root,
                force=True,
            )
        after_findings = _star_findings(data_root, day) if apply and detected else []
        remaining = sum(f.code in (MINUTE_VOLUME_X100, MINUTE_VOLUME_MISMATCH)
                        for f in after_findings)
        repaired = detected - sum(f.code == MINUTE_VOLUME_X100 for f in after_findings)
        if not apply:
            repaired = detected
        clean = detected == 0 and remaining == 0
        repairable = (
            detected >= min_per_day and repaired >= min_per_day and remaining == 0
        )
        day_ok = clean or repairable
        state = "clean" if clean else "repairable" if repairable else "invalid"
        ok = ok and day_ok
        per_day.append({
            "day": day,
            "detected": detected,
            "repaired": repaired,
            "remaining": remaining,
            "state": state,
            "ok": day_ok,
        })
    detected = sum(item.get("detected", 0) for item in per_day)
    repaired = sum(item.get("repaired", 0) for item in per_day)
    mode = "+".join(sorted({item["state"] for item in per_day})) or "invalid"
    repair_label = "实际修复" if apply else "dry-run 可修复"
    detail = f"模式={mode}，检出={detected}，{repair_label}={repaired}；{per_day}"
    return _result("PASS" if ok else "FAIL", detail,
                   detected=detected, repaired=repaired, days=per_day)


def check_special_close_days(
    data_root: Path, days: tuple[str, ...] = SPECIAL_DAYS
) -> dict[str, Any]:
    """提前收盘/特殊日的分钟-日线收盘差异必须全部为 soft。"""
    counts: dict[str, int] = {}
    hard: list[str] = []
    for day in days:
        if not all(_part(data_root, table, day).is_file()
                   for table in ("kline_minute", "kline_daily")):
            hard.append(f"{day}: 缺少分区")
            continue
        findings = compare_minute_to_daily(
            pl.read_parquet(_part(data_root, "kline_minute", day)),
            pl.read_parquet(_part(data_root, "kline_daily", day)),
            table="kline_minute",
        )
        close = [finding for finding in findings if finding.code == MINUTE_CLOSE_MISMATCH]
        counts[day] = len(close)
        hard.extend(
            f"{day}:{finding.symbol}:{finding.severity}"
            for finding in close if finding.severity != SEVERITY_SOFT
        )
    ok = not hard
    return _result(
        "PASS" if ok else "FAIL",
        f"收盘差异={counts}，非 soft={hard[:5]}",
        close_counts=counts,
        non_soft=hard,
    )


def _sample_symbols(data_root: Path, n_sym: int, seed: int) -> list[str]:
    instruments = pl.read_parquet(
        data_root / "instruments" / "instruments.parquet", columns=["symbol"]
    )
    pool = sorted(s for s in instruments["symbol"].to_list() if not s.endswith(".BJ"))
    return sorted(random.Random(seed).sample(pool, min(n_sym, len(pool))))


def _load_local_qfq_inputs(
    data_root: Path, symbols: list[str], start: str, end: str
) -> tuple[pl.DataFrame, pl.DataFrame]:
    paths = [
        path for path in sorted((data_root / "kline_daily").glob("date=*/part.parquet"))
        if start <= path.parent.name[5:] <= end
    ]
    if not paths:
        raise ValueError(f"{start}..{end} 没有本地日线分区")
    raw = (
        pl.scan_parquet([str(path) for path in paths])
        .select(["symbol", "date", "close"])
        .filter(pl.col("symbol").is_in(symbols))
        .collect()
    )
    factor_path = data_root / "adj_factor_baostock" / "all.parquet"
    factors = (
        pl.read_parquet(factor_path)
        .rename({"trade_date": "date"})
        .filter(pl.col("symbol").is_in(symbols))
        .select(["symbol", "date", "ex_factor"])
    )
    # 权威表是事件行；先按生产读侧的 backward as-of 语义展开到每个交易日，
    # 再交给 Task 12 的 apply_local_qfq 做 close × ex_factor。
    expanded = raw.select(["symbol", "date"]).sort("date").join_asof(
        factors.sort("date"), on="date", by="symbol", strategy="backward"
    )
    return raw, expanded


_QFQ_CHILD = r"""
import json
import random
import sys
from pathlib import Path

root, start, end, n_sym, seed = sys.argv[1:6]
from app.quant import daily_basis_repair as repair
repair.DATA = Path(root)
report = repair.detect(start, end, int(n_sym), seed=int(seed))

inst = repair.pl.read_parquet(root + "/instruments/instruments.parquet", columns=["symbol"])
pool = sorted(s for s in inst["symbol"].to_list() if not s.endswith(".BJ"))
random.seed(int(seed))
symbols = sorted(random.sample(pool, min(int(n_sym), len(pool))))

import baostock as bs
bs.login()
rows = []
try:
    for symbol in symbols:
        code, market = symbol.split(".")
        rs = bs.query_history_k_data_plus(
            ("sh" if market == "SH" else "sz") + "." + code,
            "date,close", start_date=start, end_date=end,
            frequency="d", adjustflag="2",
        )
        if rs.error_code != "0":
            continue
        while rs.next():
            values = rs.get_row_data()
            if values[1]:
                rows.append({"symbol": symbol, "date": values[0],
                             "qfq_close": float(values[1])})
finally:
    bs.logout()
print("__VERIFY_ACCEPTANCE_JSON__" + json.dumps(
    {"detect": report, "external": rows}, ensure_ascii=False
))
"""


def _run_qfq_child(
    data_root: Path, start: str, end: str, n_sym: int, seed: int, timeout: float
) -> dict[str, Any]:
    command = [
        sys.executable, "-c", _QFQ_CHILD,
        str(data_root), start, end, str(n_sym), str(seed),
    ]
    try:
        completed = subprocess.run(
            command, cwd=BACKEND_ROOT, capture_output=True, text=True,
            timeout=timeout, check=False,
        )
    except subprocess.TimeoutExpired:
        return _result("SKIP", f"baostock 超过 {timeout:g}s，已终止子进程")
    if completed.returncode != 0:
        return _result("FAIL", f"qfq/baostock 子进程失败: {completed.stderr[-1000:]}")
    lines = [line for line in completed.stdout.splitlines() if line.startswith(_CHILD_MARKER)]
    if not lines:
        return _result("FAIL", "qfq 子进程没有返回结构化结果")
    try:
        return _result("OK", "", payload=json.loads(lines[-1][len(_CHILD_MARKER):]))
    except (TypeError, ValueError) as exc:
        return _result("FAIL", f"qfq 子进程结果无法解析: {exc}")


def _basis_agrees(qfq_days: set[str], baseline: dict[str, Any]) -> bool:
    """两工具结论一致即通过；双方都干净也算一致。

    ``daily_basis_repair.detect`` 的 ``worst`` 只保留最严重的 5 天，因此有日期
    明细时要求它是 data_verify 异常日集合的子集，而不是强求两边集合等长。
    """
    baseline_bad = int(baseline.get("qfq_days") or 0) > 0
    if bool(qfq_days) != baseline_bad:
        return False
    if not qfq_days:
        return True
    baseline_days = {
        str(item["date"])
        for item in (baseline.get("worst") or [])
        if isinstance(item, dict) and item.get("date")
    }
    return not baseline_days or baseline_days.issubset(qfq_days)


def check_qfq_basis(
    data_root: Path,
    *,
    n_sym: int = 8,
    seed: int = 42,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """qfq 核验与 daily_basis_repair.detect 的异常/干净结论必须一致。"""
    try:
        from app.quant.data_verify.checks_factors import apply_local_qfq, compare_qfq
    except ModuleNotFoundError as exc:
        if exc.name == "app.quant.data_verify.checks_factors":
            return _result("SKIP", "Task 12 checks_factors 尚未提供")
        raise

    sampled = _sample_symbols(data_root, n_sym, seed)
    if not sampled:
        return _result("FAIL", "没有可抽样的股票")
    probed = _run_qfq_child(data_root, QFQ_START, QFQ_END, len(sampled), seed, timeout)
    if probed["status"] != "OK":
        return probed
    payload = probed["payload"]
    baseline = payload["detect"]
    external_rows = payload["external"]
    if not external_rows:
        return _result("SKIP", "baostock 没有返回抽样标的的 qfq 数据", baseline=baseline)
    external = pl.DataFrame(external_rows).with_columns(pl.col("date").str.to_date())
    raw, factors = _load_local_qfq_inputs(data_root, sampled, QFQ_START, QFQ_END)
    finding_counts: dict[str, int] = {}
    qfq_days: set[str] = set()
    for symbol in sampled:
        local_raw = raw.filter(pl.col("symbol") == symbol)
        external_symbol = external.filter(pl.col("symbol") == symbol)
        if local_raw.is_empty() or external_symbol.is_empty():
            continue
        local = apply_local_qfq(
            local_raw, factors.filter(pl.col("symbol") == symbol)
        )
        findings = compare_qfq(local, external_symbol, symbol=symbol)
        finding_counts[symbol] = len(findings)
        qfq_days.update(finding.day.isoformat() for finding in findings)
    ok = _basis_agrees(qfq_days, baseline)
    detail = (
        f"data_verify 异常标的={sum(bool(n) for n in finding_counts.values())}/{len(sampled)}，"
        f"data_verify qfq_days={len(qfq_days)}，"
        f"daily_basis_repair qfq_days={baseline.get('qfq_days')}，"
        f"qfq_range={baseline.get('qfq_range')}"
    )
    return _result("PASS" if ok else "FAIL", detail,
                   findings=finding_counts, baseline=baseline)


def _print_check(name: str, result: dict[str, Any]) -> None:
    print(f"{result['status']} {name}: {result['detail']}", flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=BACKEND_ROOT.parent / "data")
    parser.add_argument("--star-day", action="append", dest="star_days",
                        help="科创板验收日（可重复，默认 2026-11-06）")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="只读；修复数表示可修复数（默认）")
    mode.add_argument("--apply", action="store_true", help="实际修复科创板 ×100 并复查")
    parser.add_argument("--qfq-sample", type=int, default=8)
    parser.add_argument("--qfq-timeout", type=float, default=60.0)
    args = parser.parse_args(argv)

    root = args.data_root.resolve()
    checks = [
        (
            "科创板分钟量额 ×100",
            check_star_x100(
                root, args.star_days or ["2026-11-06"], apply=args.apply
            ),
        ),
        ("2023 年前复权污染", check_qfq_basis(
            root, n_sym=args.qfq_sample, timeout=args.qfq_timeout
        )),
        ("特殊交易日收盘差异", check_special_close_days(root)),
    ]
    for name, result in checks:
        _print_check(name, result)
    return int(any(result["status"] == "FAIL" for _, result in checks))


if __name__ == "__main__":
    raise SystemExit(main())
