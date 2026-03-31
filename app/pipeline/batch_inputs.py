from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from app.legacy_backend.input_projection import (
    build_legacy_backend_input_products,
)
from app.pipeline.models import ResolvedBatchInputs
from app.reference.models import enriched_snapshots_from_reference_results

if TYPE_CHECKING:
    from app.pipeline.models import (
        SupportsReferenceObserver,
        SupportsReferenceResultLoader,
    )


@dataclass(frozen=True, slots=True)
class NoOpBatchInputResolver:
    """Resolve batches that do not need enrichment or reference data."""

    def resolve(self, rows: list[dict[str, Any]]) -> ResolvedBatchInputs:
        """Return empty neutral inputs for raw-only, runtime-only execution."""
        del rows
        return ResolvedBatchInputs(
            enriched_snapshots=[],
            reference_findings=[],
            cache_hit_count=0,
            backend_run_count=0,
        )


@dataclass(frozen=True, slots=True)
class ReferenceBatchInputResolver:
    """Resolve neutral batch inputs from the current reference-side runtime."""

    reference_result_loader: SupportsReferenceResultLoader
    reference_observer: SupportsReferenceObserver
    include_enriched_snapshots: bool

    def resolve(self, rows: list[dict[str, Any]]) -> ResolvedBatchInputs:
        """Materialize only the neutral inputs that the current batch needs."""
        if not rows:
            return ResolvedBatchInputs(
                enriched_snapshots=[],
                reference_findings=[],
                cache_hit_count=0,
                backend_run_count=0,
            )

        reference_results = self.reference_result_loader.load_many(
            build_legacy_backend_input_products(rows)
        )
        return ResolvedBatchInputs(
            enriched_snapshots=(
                enriched_snapshots_from_reference_results(
                    reference_results.reference_results
                )
                if self.include_enriched_snapshots
                else []
            ),
            reference_findings=(
                self.reference_observer.observe_findings(
                    reference_results.reference_results
                )
                if self.reference_observer.requires_reference_results
                else []
            ),
            cache_hit_count=reference_results.cache_hit_count,
            backend_run_count=reference_results.backend_run_count,
        )
