from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel

if TYPE_CHECKING:
    from openfoodfacts_data_quality.contracts.checks import CheckDefinition, Severity
    from openfoodfacts_data_quality.contracts.findings import Finding

ObservationSide = Literal["reference", "migrated"]
ComparisonStatus = Literal["compared", "not_compared"]


@dataclass(frozen=True)
class ParityRunMetadata:
    """Stable metadata shared by batch and full-run parity results."""

    run_id: str
    source_snapshot_id: str
    product_count: int


class ObservedFinding(BaseModel):
    """One finding observed on one side of parity evaluation."""

    product_id: str
    check_id: str
    observed_code: str
    severity: Severity
    side: ObservationSide

    def strict_key(self) -> tuple[str, str, str]:
        """Return the tuple used by strict parity comparison."""
        return (
            self.product_id,
            self.observed_code,
            self.severity,
        )


class CheckParityResult(BaseModel):
    """Parity evaluation result for one check."""

    definition: CheckDefinition
    comparison_status: ComparisonStatus = "compared"
    reference_count: int
    migrated_count: int
    matched_count: int
    missing_count: int
    extra_count: int
    missing: list[ObservedFinding]
    extra: list[ObservedFinding]
    passed: bool | None

    @property
    def is_compared(self) -> bool:
        """Return whether this check participates in parity comparison."""
        return self.comparison_status == "compared"


class ParityResult(BaseModel):
    """Top-level parity result consumed by the application presentation layer."""

    run_id: str
    source_snapshot_id: str
    product_count: int
    checks: list[CheckParityResult]
    compared_check_count: int
    not_compared_check_count: int
    reference_total: int
    migrated_total: int
    matched_total: int
    not_compared_migrated_total: int


def _parity_types_namespace() -> dict[str, object]:
    """Return runtime types needed by Pydantic to resolve deferred annotations."""
    from openfoodfacts_data_quality.contracts.checks import CheckDefinition, Severity
    from openfoodfacts_data_quality.contracts.findings import Finding

    return {
        "CheckDefinition": CheckDefinition,
        "Severity": Severity,
        "Finding": Finding,
    }


_PARITY_TYPES_NAMESPACE = _parity_types_namespace()
ObservedFinding.model_rebuild(_types_namespace=_PARITY_TYPES_NAMESPACE)
CheckParityResult.model_rebuild(_types_namespace=_PARITY_TYPES_NAMESPACE)
ParityResult.model_rebuild(_types_namespace=_PARITY_TYPES_NAMESPACE)


def observed_migrated_finding(finding: Finding) -> ObservedFinding:
    """Adapt one library-facing finding into the parity observation model."""
    return ObservedFinding(
        product_id=finding.product_id,
        check_id=finding.check_id,
        observed_code=finding.emitted_code or finding.check_id,
        severity=finding.severity,
        side="migrated",
    )
