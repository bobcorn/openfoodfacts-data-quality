from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel

if TYPE_CHECKING:
    from off_data_quality.contracts.checks import CheckDefinition
    from off_data_quality.contracts.observations import ObservedFinding

ComparisonStatus = Literal["compared", "runtime_only"]


@dataclass(frozen=True)
class RunMetadata:
    """Stable metadata shared by batch level and full run results."""

    run_id: str
    source_snapshot_id: str
    product_count: int


class RunCheckResult(BaseModel):
    """Run result for one active check definition."""

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
        """Return whether this check participates in strict comparison."""
        return self.comparison_status == "compared"

    @property
    def is_runtime_only(self) -> bool:
        """Return whether this check is executed without a legacy baseline."""
        return self.comparison_status == "runtime_only"


class RunResult(BaseModel):
    """Top-level run result consumed by artifacts, reports, and review tooling."""

    run_id: str
    source_snapshot_id: str
    product_count: int
    checks: list[RunCheckResult]
    compared_check_count: int
    runtime_only_check_count: int
    reference_total: int
    compared_migrated_total: int
    matched_total: int
    runtime_only_migrated_total: int


def _run_types_namespace() -> dict[str, object]:
    """Return runtime types needed by Pydantic to resolve deferred annotations."""
    from off_data_quality.contracts.checks import CheckDefinition
    from off_data_quality.contracts.observations import ObservedFinding

    return {
        "CheckDefinition": CheckDefinition,
        "ObservedFinding": ObservedFinding,
    }


RunCheckResult.model_rebuild(_types_namespace=_run_types_namespace())
RunResult.model_rebuild(_types_namespace=_run_types_namespace())
