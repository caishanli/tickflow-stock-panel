"""仲裁策略（design §5.4）：把比对结论翻译成「修 / 保留 / 隔离」。

输入是 :func:`~app.quant.data_verify.checks_external.compare_daily_external`
产出的 :class:`~app.quant.data_verify.findings.Finding`（``detail["source"]``
记录来源），以及本次核验里各外部源的结论 ``verdicts``：

- ``agree_external``：该源的数据与本地不一致（即支持「外部值才是对的」）；
- ``agree_local``：该源的数据与本地一致（即本地没错）；
- ``unknown``：没有该源的有效比对（网络失败 / 无覆盖），不参与投票。

投票规则：

- 外部票多于本地票：修。外部票 ≥2 → ``confirmed``；恰好 1 票（且无本地票）
  → ``single_source``（单来源，报告里单列）；
- 票数相同（至少一票）→ 无多数，**不修**，``quarantine``；
- 本地票更多或没有外部票（全 ``unknown``）→ ``keep_local``，只报告。

``soft`` 结论（如成交额）**永不驱动修复**（见 ``findings.py`` 的严重程度契约）：
一律 ``keep_local``，即使两个外部源都「同意外部」。

``Decision.source`` 表示该动作依据的来源：``repair_local`` 取采纳的外部源
（同票时按 :data:`SOURCE_PRIORITY` 优先级高者），``quarantine`` 取发现问题的源，
``keep_local`` 固定为 ``"local"``。

注意 ``verdicts`` 是**按源（本次核验范围）**的结论，不是按 symbol 的：日线 L2
一次只比一个交易日，逐 symbol 的投票需要更细的输入（见 Task 11 的接线）。
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Final

from app.quant.data_verify.findings import SEVERITY_SOFT, Finding

#: 动作取值。
ACTION_KEEP_LOCAL: Final = "keep_local"
ACTION_REPAIR_LOCAL: Final = "repair_local"
ACTION_QUARANTINE: Final = "quarantine"

#: 外部源结论取值。
VERDICT_AGREE_LOCAL: Final = "agree_local"
VERDICT_AGREE_EXTERNAL: Final = "agree_external"
VERDICT_UNKNOWN: Final = "unknown"

VERDICTS: Final[frozenset[str]] = frozenset(
    {VERDICT_AGREE_LOCAL, VERDICT_AGREE_EXTERNAL, VERDICT_UNKNOWN}
)

#: 来源优先级（design §5.4，高→低）。未登记的来源按 0 处理，仍可投票。
SOURCE_PRIORITY: Final[dict[str, int]] = {
    "exchange": 6,
    "tickflow": 6,
    "tdx": 5,
    "baostock": 4,
    "tencent": 3,
    "sina": 2,
    "local": 1,
}


@dataclass(frozen=True)
class Decision:
    """一条仲裁结论：对 ``finding`` 采取 ``action``，依据来源 ``source``。"""

    action: str
    finding: Finding
    source: str


def _priority(source: str) -> int:
    """来源优先级；未登记的来源给 0（票仍然算，但同票时排后面）。"""
    return SOURCE_PRIORITY.get(source, 0)


def _sorted_sources(sources: list[str]) -> list[str]:
    """按 design §5.4 优先级降序、同优先级按名字升序（结果确定）。"""
    return sorted(sources, key=lambda source: (-_priority(source), source))


def _validate_verdicts(verdicts: dict[str, str]) -> None:
    invalid = {
        source: verdict for source, verdict in verdicts.items()
        if verdict not in VERDICTS
    }
    if invalid:
        raise ValueError(
            f"verdicts 含非法结论 {invalid}；允许值 {sorted(VERDICTS)}"
        )


def _with_detail(finding: Finding, **extra: Any) -> Finding:
    """``frozen`` 的 :class:`Finding` 不能就地改，返回带补充字段的副本。"""
    return replace(finding, detail={**finding.detail, **extra})


def decide(findings: list[Finding], verdicts: dict[str, str]) -> list[Decision]:
    """按 §5.4 多数规则为每条 finding 产出一条 :class:`Decision`。

    ``verdicts`` 缺失的源视为 ``unknown``；出现不认识的结论值直接
    ``ValueError``（拼错 ``agree-external`` 之类不能被静默当成弃权）。
    """
    _validate_verdicts(verdicts)
    external_sources = _sorted_sources(
        [source for source, verdict in verdicts.items() if verdict == VERDICT_AGREE_EXTERNAL]
    )
    local_sources = _sorted_sources(
        [source for source, verdict in verdicts.items() if verdict == VERDICT_AGREE_LOCAL]
    )
    n_external = len(external_sources)
    n_local = len(local_sources)

    decisions: list[Decision] = []
    for finding in findings:
        own_source = str(finding.detail.get("source") or "")

        if finding.severity == SEVERITY_SOFT:
            # 口径差异只报告：成交额这类字段跨源本来就不完全一致。
            decisions.append(Decision(ACTION_KEEP_LOCAL, finding, "local"))
            continue

        if n_external == 0 or n_external < n_local:
            decisions.append(Decision(ACTION_KEEP_LOCAL, finding, "local"))
            continue

        if n_external == n_local:
            # 无多数（一票对一票等）：不修，进隔离清单（design §6.3）。
            decisions.append(
                Decision(
                    ACTION_QUARANTINE,
                    _with_detail(
                        finding,
                        conflict=True,
                        agree_external_sources=list(external_sources),
                        agree_local_sources=list(local_sources),
                    ),
                    own_source or external_sources[0],
                )
            )
            continue

        if n_external >= 2:
            detail = {"confirmed": True, "external_sources": list(external_sources)}
        else:
            detail = {"single_source": True, "external_sources": list(external_sources)}
        decisions.append(
            Decision(
                ACTION_REPAIR_LOCAL,
                _with_detail(finding, **detail),
                external_sources[0],
            )
        )

    return decisions


__all__ = [
    "ACTION_KEEP_LOCAL",
    "ACTION_QUARANTINE",
    "ACTION_REPAIR_LOCAL",
    "SOURCE_PRIORITY",
    "VERDICT_AGREE_EXTERNAL",
    "VERDICT_AGREE_LOCAL",
    "VERDICT_UNKNOWN",
    "Decision",
    "decide",
]
