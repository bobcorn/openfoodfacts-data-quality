from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, ParamSpec, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable

    from app.legacy_backend.input_projection import LegacyBackendInputProduct
    from app.parity.models import ObservedFinding, ParityResult
    from app.pipeline.context_builders import SupportsCheckContextBuilder
    from app.pipeline.profiles import ActiveCheckProfile
    from app.reference.models import ReferenceResult
    from openfoodfacts_data_quality.checks.registry import CheckEvaluator
    from openfoodfacts_data_quality.contracts.checks import (
        CheckDefinition,
        CheckParityBaseline,
    )
    from openfoodfacts_data_quality.contracts.enrichment import EnrichedSnapshotResult

_SubmitParams = ParamSpec("_SubmitParams")


@dataclass(frozen=True)
class BatchExecutionResult:
    """Result of running one batch through the strict parity pipeline."""

    batch_index: int
    row_count: int
    cache_hit_count: int
    backend_run_count: int
    reference_finding_count: int
    migrated_finding_count: int
    parity_result: ParityResult
    elapsed_seconds: float


@dataclass(frozen=True)
class ResolvedReferenceResults:
    """Ordered reference results together with execution telemetry."""

    reference_results: list[ReferenceResult]
    cache_hit_count: int
    backend_run_count: int


@dataclass(frozen=True)
class ResolvedBatchInputs:
    """Neutral batch inputs consumed by the execution core."""

    enriched_snapshots: list[EnrichedSnapshotResult]
    reference_findings: list[ObservedFinding]
    cache_hit_count: int
    backend_run_count: int


@dataclass(frozen=True)
class PreparedRun:
    """Normalized execution inputs derived before batch processing starts."""

    source_snapshot_id: str
    run_id: str
    product_count: int
    active_check_profile: ActiveCheckProfile
    check_context_builder: SupportsCheckContextBuilder
    reference_observer: SupportsReferenceObserver
    evaluators: dict[str, CheckEvaluator]
    reference_result_cache_key: str
    reference_result_cache_path: Path
    python_count: int
    dsl_count: int
    legacy_parity_count: int
    runtime_only_count: int

    def with_reference_result_cache(
        self,
        *,
        result_cache_key: str,
        result_cache_path: Path,
    ) -> PreparedRun:
        """Return this prepared run with the chosen backend-result cache location."""
        return PreparedRun(
            source_snapshot_id=self.source_snapshot_id,
            run_id=self.run_id,
            product_count=self.product_count,
            active_check_profile=self.active_check_profile,
            check_context_builder=self.check_context_builder,
            reference_observer=self.reference_observer,
            evaluators=self.evaluators,
            reference_result_cache_key=result_cache_key,
            reference_result_cache_path=result_cache_path,
            python_count=self.python_count,
            dsl_count=self.dsl_count,
            legacy_parity_count=self.legacy_parity_count,
            runtime_only_count=self.runtime_only_count,
        )

    @property
    def requires_enriched_snapshots(self) -> bool:
        """Return whether migrated checks need enriched snapshots for this run."""
        return self.check_context_builder.requires_enriched_snapshots

    @property
    def requires_reference_findings(self) -> bool:
        """Return whether parity needs reference findings for this run."""
        return self.reference_observer.requires_reference_results

    @property
    def requires_reference_results(self) -> bool:
        """Return whether this run needs reference results at all."""
        return self.requires_enriched_snapshots or self.requires_reference_findings


@dataclass(frozen=True)
class BatchRunPlan:
    """Static batch-loop configuration for one pipeline run."""

    db_path: Path
    batch_size: int
    legacy_backend_workers: int


@dataclass(frozen=True)
class BatchExecutionContext:
    """Shared services and metadata needed by every batch execution."""

    batch_input_resolver: SupportsBatchInputResolver
    evaluators: dict[str, CheckEvaluator]
    active_checks: tuple[CheckDefinition, ...]
    check_context_builder: SupportsCheckContextBuilder
    run_id: str
    source_snapshot_id: str


@dataclass(frozen=True)
class ScheduledBatch:
    """One submitted batch of source rows."""

    batch_index: int
    rows: list[dict[str, Any]]


class SupportsBatchInputResolver(Protocol):
    """Minimal neutral input-resolution surface needed by one batch execution."""

    def resolve(self, rows: list[dict[str, Any]]) -> ResolvedBatchInputs: ...


class SupportsBatchExecutor(Protocol):
    """Minimal executor surface needed by the batch scheduler."""

    def submit(
        self,
        _fn: Callable[_SubmitParams, BatchExecutionResult],
        /,
        *args: _SubmitParams.args,
        **kwargs: _SubmitParams.kwargs,
    ) -> Future[BatchExecutionResult]: ...


class SupportsReferenceResultLoader(Protocol):
    """Minimal reference-result loading surface needed by a batch execution."""

    def load_many(
        self,
        backend_input_products: list[LegacyBackendInputProduct],
    ) -> ResolvedReferenceResults: ...


class SupportsLegacyBackendRunner(Protocol):
    """Minimal backend surface needed by the reference-result loader."""

    def run(
        self,
        backend_input_products: list[LegacyBackendInputProduct],
    ) -> list[ReferenceResult]: ...


class SupportsReferenceResultCache(Protocol):
    """Minimal cache surface needed by the reference-result loader."""

    def load_many(self, codes: list[str]) -> dict[str, ReferenceResult]: ...

    def store_many(self, reference_results: list[ReferenceResult]) -> None: ...


class SupportsReferenceObserver(Protocol):
    """Reference observation strategy selected for one pipeline run."""

    @property
    def requires_reference_results(self) -> bool: ...

    @property
    def parity_baselines(self) -> tuple[CheckParityBaseline, ...]: ...

    def observe_findings(
        self,
        reference_results: list[ReferenceResult],
    ) -> list[ObservedFinding]: ...


class SupportsExecutionProgress(Protocol):
    """Minimal progress-reporting surface needed by the batch loop."""

    @property
    def heartbeat_interval_seconds(self) -> float: ...

    def log_heartbeat(
        self,
        *,
        processed_products: int,
        buffered_results: tuple[BatchExecutionResult, ...],
        merged_batch_count: int,
        in_flight_count: int,
    ) -> None: ...

    def log_batch_completed(
        self,
        batch_result: BatchExecutionResult,
        *,
        processed_products: int,
    ) -> None: ...


class SupportsParityAccumulator(Protocol):
    """Minimal accumulation surface needed by the batch loop."""

    def add_batch(self, batch_result: ParityResult) -> None: ...
