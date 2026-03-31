from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from app.parity.models import ObservedFinding
from openfoodfacts_data_quality.checks.legacy import LegacyCheckIndex
from openfoodfacts_data_quality.contracts.checks import SEVERITY_ORDER
from openfoodfacts_data_quality.progress import iter_with_progress

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from app.reference.models import LegacyCheckTags, ReferenceResult
    from openfoodfacts_data_quality.contracts.checks import CheckDefinition, Severity

RAW_TAG_FIELDS: tuple[Severity, ...] = (
    "bug",
    "info",
    "completeness",
    "warning",
    "error",
)
LOGGER = logging.getLogger(__name__)


def emit_reference_findings(
    reference_results: list[ReferenceResult],
    *,
    active_checks: Sequence[CheckDefinition],
    log_progress: bool = True,
) -> list[ObservedFinding]:
    """Map raw legacy backend tags into normalized reference findings for parity."""
    findings_by_product_check_and_code: dict[tuple[str, str, str], ObservedFinding] = {}
    selected_checks = tuple(active_checks)
    legacy_check_index = LegacyCheckIndex.build(selected_checks)

    reference_result_iterable = (
        iter_with_progress(
            reference_results,
            desc="Reference Path | Normalize findings",
            unit="product",
            logger=LOGGER,
        )
        if log_progress
        else reference_results
    )

    for reference_result in reference_result_iterable:
        for finding in _iter_reference_findings(
            reference_result,
            legacy_check_index=legacy_check_index,
        ):
            _store_highest_severity(findings_by_product_check_and_code, finding)

    return _sort_findings(findings_by_product_check_and_code.values())


def _iter_reference_findings(
    reference_result: ReferenceResult,
    *,
    legacy_check_index: LegacyCheckIndex,
) -> Iterator[ObservedFinding]:
    """Yield all normalized reference findings for one reference result."""
    for raw_code, severity in _iter_raw_legacy_tags(reference_result.legacy_check_tags):
        for check in legacy_check_index.match_observed_code(raw_code):
            yield _build_reference_finding(
                reference_result.code,
                check,
                raw_code,
                severity,
            )


def _iter_raw_legacy_tags(tags: LegacyCheckTags) -> Iterator[tuple[str, Severity]]:
    """Yield non-empty raw legacy codes together with their severity bucket."""
    for severity in RAW_TAG_FIELDS:
        for raw_code in getattr(tags, severity):
            if raw_code:
                yield raw_code, severity


def _build_reference_finding(
    product_id: str,
    check: CheckDefinition,
    raw_code: str,
    severity: Severity,
) -> ObservedFinding:
    """Build one normalized reference finding."""
    return ObservedFinding(
        product_id=product_id,
        check_id=check.id,
        observed_code=raw_code,
        severity=severity,
        side="reference",
    )


def _store_highest_severity(
    findings_by_product_check_and_code: dict[tuple[str, str, str], ObservedFinding],
    finding: ObservedFinding,
) -> None:
    """Keep only the highest-severity finding per product/check/code tuple."""
    key = (finding.product_id, finding.check_id, finding.observed_code)
    existing = findings_by_product_check_and_code.get(key)
    if (
        existing is None
        or SEVERITY_ORDER[finding.severity] > SEVERITY_ORDER[existing.severity]
    ):
        findings_by_product_check_and_code[key] = finding


def _sort_findings(findings: Iterable[ObservedFinding]) -> list[ObservedFinding]:
    """Return findings sorted for stable report rendering."""
    return sorted(
        findings,
        key=lambda finding: (
            finding.check_id,
            finding.product_id,
            finding.observed_code,
            finding.severity,
        ),
    )
