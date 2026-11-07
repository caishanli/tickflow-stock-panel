"""CLI：``python -m app.quant.data_verify --stage internal [--apply] [--days ...]``。

默认是 **dry-run**（只报告，不写盘）；``--apply`` 才会在备份之后做 ×100 还原。
stdout 只输出一行 JSON 摘要（便于脚本消费），进度/告警走 stderr 的日志。
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date

from app.quant.data_verify.runner import STAGES, run_verify


def _parse_days(raw: str | None) -> list[date] | None:
    """``"2026-11-05,2026-11-06"`` → ``[date(...), date(...)]``；空 → ``None``。"""
    if not raw:
        return None
    days: list[date] = []
    for token in raw.split(","):
        token = token.strip()
        if token:
            days.append(date.fromisoformat(token))
    return days or None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.quant.data_verify",
        description="多源数据核验：L0 结构 / L1 分钟→日线；默认 dry-run。",
    )
    parser.add_argument("--stage", required=True, choices=STAGES, help="核验层")
    parser.add_argument("--apply", action="store_true",
                        help="执行修复（仅 MINUTE_VOLUME_X100，写前备份）")
    parser.add_argument("--days", default=None,
                        help="逗号分隔的交易日（如 2026-11-06）；省略=全部已有分区")
    parser.add_argument("--resume", action="store_true",
                        help="从状态文件 last_day_done 之后继续")
    parser.add_argument("--data-root", default=None,
                        help="数据根目录（默认 mootdx_service.DATA_ROOT）")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        days = _parse_days(args.days)
    except ValueError as exc:
        parser.error(f"--days 需要 YYYY-MM-DD 列表: {exc}")

    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    report = run_verify(
        args.stage,
        apply=args.apply,
        days=days,
        data_root=args.data_root,
        resume=args.resume,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
