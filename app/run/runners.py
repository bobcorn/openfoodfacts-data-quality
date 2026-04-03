from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.parity.comparator import evaluate_parity
from app.run.models import ResolvedReferenceBatch
from openfoodfacts_data_quality.checks.engine import (
    CheckRunOptions,
    iter_check_findings_with_evaluators,
)
from openfoodfacts_data_quality.contracts.observations import (
    observed_migrated_finding,
)
from openfoodfacts_data_quality.contracts.run import RunMetadata

if TYPE_CHECKING:
    from collections.abc import Iterable

    from app.run.context_builders import SupportsCheckContextBuilder
    from app.run.models import (
        SupportsEnrichedSnapshotMaterializer,
        SupportsReferenceFindingMaterializer,
        SupportsReferenceResultLoader,
    )
    from openfoodfacts_data_quality.checks.registry import CheckEvaluator
    from openfoodfacts_data_quality.contracts.checks import CheckDefinition
    from openfoodfacts_data_quality.contracts.enrichment import EnrichedSnapshotResult
    from openfoodfacts_data_quality.contracts.observations import ObservedFinding
    from openfoodfacts_data_quality.contracts.raw import RawProductRow
    from openfoodfacts_data_quality.contracts.run import RunResult


@dataclass(frozen=True, slots=True)
class NoReferenceRunner:
    """Reference-side runner used when the active run has no reference path."""

    def resolve(
        self,
        rows: list[RawProductRow],
    ) -> ResolvedReferenceBatch:
        """Return an empty reference-side batch projection."""
        del rows
        return ResolvedReferenceBatch(
            reference_results=[],
            enriched_snapshots=[],
            reference_findings=(),
            cache_hit_count=0,
            backend_run_count=0,
        )


@dataclass(frozen=True, slots=True)
class LegacyReferenceRunner:
    """Resolve one batch through the reference path and derived projections."""

    reference_result_loader: SupportsReferenceResultLoader
    enriched_snapshot_materializer: SupportsEnrichedSnapshotMaterializer | None = None
    reference_finding_materializer: SupportsReferenceFindingMaterializer | None = None

    def resolve(
        self,
        rows: list[RawProductRow],
    ) -> ResolvedReferenceBatch:
        """Return the reference-side data needed for one batch execution."""
        loaded = self.reference_result_loader.load_many(rows)
        reference_results = loaded.reference_results
        return ResolvedReferenceBatch(
            reference_results=reference_results,
            enriched_snapshots=(
                self.enriched_snapshot_materializer.materialize(reference_results)
                if self.enriched_snapshot_materializer is not None
                else []
            ),
            reference_findings=(
                tuple(
                    self.reference_finding_materializer.materialize(reference_results)
                )
                if self.reference_finding_materializer is not None
                else ()
            ),
            cache_hit_count=loaded.cache_hit_count,
            backend_run_count=loaded.backend_run_count,
        )


@dataclass(frozen=True, slots=True)
class MigratedRunner:
    """Run migrated checks for one batch on the selected normalized surface."""

    check_context_builder: SupportsCheckContextBuilder
    evaluators: dict[str, CheckEvaluator]

    def observe_findings(
        self,
        *,
        rows: list[RawProductRow],
        enriched_snapshots: list[EnrichedSnapshotResult],
    ) -> Iterable[ObservedFinding]:
        """Yield normalized migrated findings for one batch."""
        return (
            observed_migrated_finding(finding)
            for finding in iter_check_findings_with_evaluators(
                self.check_context_builder.iter_contexts(
                    rows=rows,
                    enriched_snapshots=enriched_snapshots,
                ),
                self.evaluators,
                options=CheckRunOptions(
                    log_loaded=False,
                    log_progress=False,
                ),
            )
        )


@dataclass(frozen=True, slots=True)
class ParityRunner:
    """Run strict comparison for one prepared application execution."""

    run_id: str
    source_snapshot_id: str
    active_checks: tuple[CheckDefinition, ...]

    def compare(
        self,
        *,
        product_count: int,
        reference_findings: Iterable[ObservedFinding],
        migrated_findings: Iterable[ObservedFinding],
    ) -> RunResult:
        """Return the strict parity result for one processed batch."""
        return evaluate_parity(
            reference_findings=reference_findings,
            migrated_findings=migrated_findings,
            run=RunMetadata(
                run_id=self.run_id,
                source_snapshot_id=self.source_snapshot_id,
                product_count=product_count,
            ),
            checks=self.active_checks,
        )
