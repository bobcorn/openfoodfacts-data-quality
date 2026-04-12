from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING

from migration.parity.comparator import evaluate_parity
from migration.run.models import ResolvedReferenceBatch
from off_data_quality.contracts.observations import (
    observed_migrated_finding,
)
from off_data_quality.contracts.run import RunMetadata
from off_data_quality.execution import (
    CheckRunOptions,
    iter_check_findings_with_evaluators,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from migration.run.context_builders import SupportsCheckContextBuilder
    from migration.run.models import (
        SupportsReferenceCheckContextMaterializer,
        SupportsReferenceFindingMaterializer,
        SupportsReferenceResultLoader,
    )
    from migration.source.models import ProductDocument
    from off_data_quality.contracts.checks import CheckDefinition
    from off_data_quality.contracts.context import CheckContext
    from off_data_quality.contracts.observations import ObservedFinding
    from off_data_quality.contracts.run import RunResult
    from off_data_quality.execution import CheckEvaluator


@dataclass(frozen=True, slots=True)
class NoReferenceRunner:
    """Reference-side runner used when the active run has no reference path."""

    def resolve(
        self,
        product_documents: list[ProductDocument],
    ) -> ResolvedReferenceBatch:
        """Return an empty reference-side batch projection."""
        del product_documents
        return ResolvedReferenceBatch(
            reference_check_contexts=[],
            reference_findings=(),
            cache_hit_count=0,
            backend_run_count=0,
            load_seconds=0.0,
            reference_check_context_materialization_seconds=0.0,
            reference_finding_materialization_seconds=0.0,
        )


@dataclass(frozen=True, slots=True)
class LegacyReferenceRunner:
    """Resolve one batch through the reference path and derived projections."""

    reference_result_loader: SupportsReferenceResultLoader
    reference_check_context_materializer: (
        SupportsReferenceCheckContextMaterializer | None
    ) = None
    reference_finding_materializer: SupportsReferenceFindingMaterializer | None = None

    def resolve(
        self,
        product_documents: list[ProductDocument],
    ) -> ResolvedReferenceBatch:
        """Return the reference-side data needed for one batch execution."""
        loaded = self.reference_result_loader.load_many(product_documents)
        reference_results = loaded.reference_results
        reference_check_context_materialization_seconds = 0.0
        reference_finding_materialization_seconds = 0.0
        reference_check_contexts: list[CheckContext] = []
        reference_findings: tuple[ObservedFinding, ...] = ()

        if self.reference_check_context_materializer is not None:
            started = perf_counter()
            reference_check_contexts = (
                self.reference_check_context_materializer.materialize(reference_results)
            )
            reference_check_context_materialization_seconds = perf_counter() - started

        if self.reference_finding_materializer is not None:
            started = perf_counter()
            reference_findings = tuple(
                self.reference_finding_materializer.materialize(reference_results)
            )
            reference_finding_materialization_seconds = perf_counter() - started

        return ResolvedReferenceBatch(
            reference_check_contexts=reference_check_contexts,
            reference_findings=reference_findings,
            cache_hit_count=loaded.cache_hit_count,
            backend_run_count=loaded.backend_run_count,
            load_seconds=loaded.load_seconds,
            reference_check_context_materialization_seconds=(
                reference_check_context_materialization_seconds
            ),
            reference_finding_materialization_seconds=(
                reference_finding_materialization_seconds
            ),
        )


@dataclass(frozen=True, slots=True)
class MigratedRunner:
    """Run migrated checks for one batch on the selected context provider."""

    check_context_builder: SupportsCheckContextBuilder
    evaluators: dict[str, CheckEvaluator]

    def observe_findings(
        self,
        *,
        reference_check_contexts: list[CheckContext],
    ) -> Iterable[ObservedFinding]:
        """Yield normalized migrated findings for one batch."""
        return (
            observed_migrated_finding(finding)
            for finding in iter_check_findings_with_evaluators(
                self.check_context_builder.iter_contexts(
                    reference_check_contexts=reference_check_contexts,
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
    """Run strict comparison for one prepared migration execution."""

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
