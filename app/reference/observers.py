from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.reference.findings import iter_reference_findings

if TYPE_CHECKING:
    from collections.abc import Iterable

    from app.reference.models import ReferenceResult
    from openfoodfacts_data_quality.contracts.checks import (
        CheckDefinition,
        CheckParityBaseline,
    )
    from openfoodfacts_data_quality.contracts.observations import ObservedFinding


@dataclass(frozen=True, slots=True)
class NoReferenceObserver:
    """Reference observer used when no active check participates in parity."""

    parity_baselines: tuple[CheckParityBaseline, ...] = ("none",)

    @property
    def requires_reference_results(self) -> bool:
        return False

    def observe_findings(
        self,
        reference_results: list[ReferenceResult],
    ) -> Iterable[ObservedFinding]:
        del reference_results
        return ()


@dataclass(frozen=True, slots=True)
class LegacyReferenceObserver:
    """Reference observer that normalizes legacy backend findings for parity."""

    active_checks: tuple[CheckDefinition, ...]
    parity_baselines: tuple[CheckParityBaseline, ...] = ("legacy",)

    @property
    def requires_reference_results(self) -> bool:
        return True

    def observe_findings(
        self,
        reference_results: list[ReferenceResult],
    ) -> Iterable[ObservedFinding]:
        return iter_reference_findings(
            reference_results,
            active_checks=self.active_checks,
            log_progress=False,
        )


def reference_observer_for(
    active_checks: tuple[CheckDefinition, ...],
) -> LegacyReferenceObserver | NoReferenceObserver:
    """Return the reference observation strategy implied by the active checks."""
    legacy_checks = tuple(
        check for check in active_checks if check.parity_baseline == "legacy"
    )
    if legacy_checks:
        return LegacyReferenceObserver(active_checks=legacy_checks)
    return NoReferenceObserver()
