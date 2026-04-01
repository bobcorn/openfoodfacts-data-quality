from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.reference.models import enriched_snapshots_from_reference_results

if TYPE_CHECKING:
    from collections.abc import Iterable

    from app.reference.models import ReferenceResult
    from app.run.models import SupportsReferenceObserver
    from openfoodfacts_data_quality.contracts.enrichment import (
        EnrichedSnapshotResult,
    )
    from openfoodfacts_data_quality.contracts.observations import ObservedFinding


@dataclass(frozen=True, slots=True)
class EnrichedSnapshotMaterializer:
    """Project reference results onto the enriched runtime input surface."""

    def materialize(
        self,
        reference_results: list[ReferenceResult],
    ) -> list[EnrichedSnapshotResult]:
        """Return stable enriched snapshots for the migrated runtime."""
        return enriched_snapshots_from_reference_results(reference_results)


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
