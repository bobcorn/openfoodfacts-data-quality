from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from migration.reference.models import reference_check_contexts_from_reference_results

if TYPE_CHECKING:
    from collections.abc import Iterable

    from migration.reference.models import ReferenceResult
    from migration.run.models import SupportsReferenceObserver
    from off_data_quality.contracts.context import CheckContext
    from off_data_quality.contracts.observations import ObservedFinding


@dataclass(frozen=True, slots=True)
class ReferenceCheckContextMaterializer:
    """Project reference results into enriched check contexts for comparison."""

    def materialize(
        self,
        reference_results: list[ReferenceResult],
    ) -> list[CheckContext]:
        """Return enriched check contexts built from reference results."""
        return reference_check_contexts_from_reference_results(reference_results)


@dataclass(frozen=True, slots=True)
class ReferenceFindingMaterializer:
    """Project reference results onto normalized reference findings for parity."""

    reference_observer: SupportsReferenceObserver

    def materialize(
        self,
        reference_results: list[ReferenceResult],
    ) -> Iterable[ObservedFinding]:
        """Return normalized reference findings for strict comparison."""
        return self.reference_observer.observe_findings(reference_results)
