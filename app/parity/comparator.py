from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from openfoodfacts_data_quality.contracts.observations import ObservedFinding
from openfoodfacts_data_quality.contracts.run import (
    RunCheckResult,
    RunMetadata,
    RunResult,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from openfoodfacts_data_quality.contracts.checks import CheckDefinition


def evaluate_parity(
    reference_findings: Iterable[ObservedFinding],
    migrated_findings: Iterable[ObservedFinding],
    run: RunMetadata,
    *,
    checks: Sequence[CheckDefinition],
) -> RunResult:
    """Evaluate strict comparison across reference and migrated findings."""
    active_checks = tuple(checks)

    reference_by_check, reference_total = _group_findings_by_check(reference_findings)
    migrated_by_check, _migrated_total = _group_findings_by_check(migrated_findings)

    run_checks: list[RunCheckResult] = []
    compared_migrated_total = 0
    matched_total = 0
    runtime_only_migrated_total = 0

    for check in active_checks:
        reference_items = sorted(
            _findings_for_check(reference_by_check, check.id),
            key=_finding_strict_key,
        )
        migrated_items = sorted(
            _findings_for_check(migrated_by_check, check.id),
            key=_finding_strict_key,
        )

        if check.parity_baseline == "none":
            runtime_only_migrated_total += len(migrated_items)
            run_checks.append(
                RunCheckResult(
                    definition=check,
                    comparison_status="runtime_only",
                    reference_count=0,
                    migrated_count=len(migrated_items),
                    matched_count=0,
                    missing_count=0,
                    extra_count=0,
                    missing=[],
                    extra=[],
                    passed=None,
                )
            )
            continue

        matched_count, missing_items, extra_items = _diff_observed_findings(
            reference_items,
            migrated_items,
        )

        compared_migrated_total += len(migrated_items)
        matched_total += matched_count

        run_checks.append(
            RunCheckResult(
                definition=check,
                comparison_status="compared",
                reference_count=len(reference_items),
                migrated_count=len(migrated_items),
                matched_count=matched_count,
                missing_count=len(missing_items),
                extra_count=len(extra_items),
                missing=missing_items,
                extra=extra_items,
                passed=not missing_items and not extra_items,
            )
        )

    return RunResult(
        run_id=run.run_id,
        source_snapshot_id=run.source_snapshot_id,
        product_count=run.product_count,
        checks=run_checks,
        compared_check_count=sum(1 for check in run_checks if check.is_compared),
        runtime_only_check_count=sum(
            1 for check in run_checks if check.is_runtime_only
        ),
        reference_total=reference_total,
        compared_migrated_total=compared_migrated_total,
        matched_total=matched_total,
        runtime_only_migrated_total=runtime_only_migrated_total,
    )


def _group_findings_by_check(
    findings: Iterable[ObservedFinding],
) -> tuple[dict[str, list[ObservedFinding]], int]:
    """Group observed findings by canonical check id."""
    grouped: defaultdict[str, list[ObservedFinding]] = defaultdict(list)
    total = 0
    for finding in findings:
        grouped[finding.check_id].append(finding)
        total += 1
    return dict(grouped), total


def _findings_for_check(
    findings_by_check: dict[str, list[ObservedFinding]],
    check_id: str,
) -> list[ObservedFinding]:
    """Return the grouped findings for one check with a typed empty fallback."""
    return findings_by_check.get(check_id, [])


def _finding_strict_key(item: ObservedFinding) -> tuple[str, str, str]:
    """Return the multiset identity used for strict comparison."""
    return item.strict_key()


def _diff_observed_findings(
    reference_items: list[ObservedFinding],
    migrated_items: list[ObservedFinding],
) -> tuple[int, list[ObservedFinding], list[ObservedFinding]]:
    """Compare findings as multisets, preserving duplicate occurrences."""
    reference_by_key: dict[tuple[str, str, str], list[ObservedFinding]] = defaultdict(
        list
    )
    migrated_by_key: dict[tuple[str, str, str], list[ObservedFinding]] = defaultdict(
        list
    )

    for item in reference_items:
        reference_by_key[item.strict_key()].append(item)
    for item in migrated_items:
        migrated_by_key[item.strict_key()].append(item)

    matched_count = 0
    missing_items: list[ObservedFinding] = []
    extra_items: list[ObservedFinding] = []

    for key in sorted(set(reference_by_key) | set(migrated_by_key)):
        reference_group = reference_by_key.get(key, [])
        migrated_group = migrated_by_key.get(key, [])
        shared_count = min(len(reference_group), len(migrated_group))
        matched_count += shared_count
        missing_items.extend(reference_group[shared_count:])
        extra_items.extend(migrated_group[shared_count:])

    return matched_count, missing_items, extra_items
