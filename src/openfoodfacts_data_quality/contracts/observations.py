from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel

if TYPE_CHECKING:
    from openfoodfacts_data_quality.contracts.checks import Severity
    from openfoodfacts_data_quality.contracts.findings import Finding

ObservationSide = Literal["reference", "migrated"]


class ObservedFinding(BaseModel):
    """One normalized finding observation on one execution side."""

    product_id: str
    check_id: str
    observed_code: str
    severity: Severity
    side: ObservationSide

    def strict_key(self) -> tuple[str, str, str]:
        """Return the tuple used by strict comparison."""
        return (
            self.product_id,
            self.observed_code,
            self.severity,
        )


def observed_migrated_finding(finding: Finding) -> ObservedFinding:
    """Adapt one runtime finding into the normalized observed shape."""
    return ObservedFinding(
        product_id=finding.product_id,
        check_id=finding.check_id,
        observed_code=finding.emitted_code or finding.check_id,
        severity=finding.severity,
        side="migrated",
    )


def _observed_finding_types_namespace() -> dict[str, object]:
    """Return runtime types needed by Pydantic to resolve deferred annotations."""
    from openfoodfacts_data_quality.contracts.checks import Severity
    from openfoodfacts_data_quality.contracts.findings import Finding

    return {
        "Severity": Severity,
        "Finding": Finding,
    }


ObservedFinding.model_rebuild(_types_namespace=_observed_finding_types_namespace())
