from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, ParamSpec, Protocol

from app.migration.catalog import ActiveMigrationPlan
from app.source.datasets import (
    ActiveDatasetProfile,
    SourceSelection,
    default_dataset_profile,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from app.artifacts import ApplicationArtifacts
    from app.legacy_backend.input_projection import LegacyBackendInputProduct
    from app.reference.models import ReferenceResult
    from app.run.context_builders import SupportsCheckContextBuilder
    from app.run.profiles import ActiveCheckProfile
    from openfoodfacts_data_quality.checks.registry import CheckEvaluator
    from openfoodfacts_data_quality.contracts.checks import CheckParityBaseline
    from openfoodfacts_data_quality.contracts.enrichment import EnrichedSnapshotResult
    from openfoodfacts_data_quality.contracts.observations import ObservedFinding
    from openfoodfacts_data_quality.contracts.raw import RawProductRow
    from openfoodfacts_data_quality.contracts.run import RunResult

_SubmitParams = ParamSpec("_SubmitParams")


@dataclass(frozen=True)
class BatchExecutionResult:
    """Result of running one batch through the current run loop."""

    batch_index: int
    row_count: int
    cache_hit_count: int
    backend_run_count: int
    reference_finding_count: int
    migrated_finding_count: int
    run_result: RunResult
    elapsed_seconds: float


@dataclass(frozen=True)
class ResolvedReferenceResults:
    """Ordered reference results together with execution telemetry."""

    reference_results: list[ReferenceResult]
    cache_hit_count: int
    backend_run_count: int


@dataclass(frozen=True, slots=True)
class ResolvedReferenceBatch:
    """Reference-side batch inputs after cache/backend loading and projection."""

    reference_results: list[ReferenceResult]
    enriched_snapshots: list[EnrichedSnapshotResult]
    reference_findings: tuple[ObservedFinding, ...]
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
    reference_result_cache_key: str | None
    reference_result_cache_path: Path | None
    python_count: int
    dsl_count: int
    legacy_parity_count: int
    runtime_only_count: int
    active_dataset_profile: ActiveDatasetProfile = field(
        default_factory=default_dataset_profile
    )
    active_migration_plan: ActiveMigrationPlan = field(
        default_factory=ActiveMigrationPlan
    )

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
            active_dataset_profile=self.active_dataset_profile,
            active_migration_plan=self.active_migration_plan,
        )

    @property
    def requires_enriched_snapshots(self) -> bool:
        """Return whether migrated checks need enriched snapshots for this run."""
        return self.check_context_builder.requires_enriched_snapshots

    @property
    def requires_reference_findings(self) -> bool:
        """Return whether strict comparison needs reference findings for this run."""
        return self.reference_observer.requires_reference_results

    @property
    def requires_reference_results(self) -> bool:
        """Return whether this run needs reference results at all."""
        return self.requires_enriched_snapshots or self.requires_reference_findings


@dataclass(frozen=True, slots=True)
class ExecutedApplicationRun:
    """Completed application execution plus the prepared artifact workspace."""

    run_result: RunResult
    artifacts: ApplicationArtifacts


@dataclass(frozen=True, slots=True)
class RunSpec:
    """Explicit application run specification resolved before orchestration starts."""

    project_root: Path
    db_path: Path
    batch_size: int
    mismatch_examples_limit: int
    batch_workers: int
    legacy_backend_workers: int
    reference_result_cache_dir: Path
    reference_result_cache_salt: str
    check_profile_name: str | None = None
    parity_store_path: Path | None = None
    dataset_profile_name: str | None = None
    legacy_inventory_artifact_path: Path | None = None
    legacy_estimation_sheet_path: Path | None = None

    @property
    def profile_config_path(self) -> Path:
        """Return the shipped check-profile config path."""
        return self.project_root / "config" / "check-profiles.toml"

    @property
    def dataset_profile_config_path(self) -> Path:
        """Return the shipped dataset-profile config path."""
        return self.project_root / "config" / "dataset-profiles.toml"


@dataclass(frozen=True, slots=True)
class PreviewSettings:
    """Settings used by the local static-preview entrypoint."""

    port: int


@dataclass(frozen=True)
class BatchRunPlan:
    """Static batch-loop configuration for one application run."""

    db_path: Path
    batch_size: int
    batch_workers: int
    legacy_backend_workers: int
    source_selection: SourceSelection


@dataclass(frozen=True)
class BatchExecutionContext:
    """Shared services and metadata needed by every batch execution."""

    reference_runner: SupportsReferenceRunner
    migrated_runner: SupportsMigratedRunner
    parity_runner: SupportsParityRunner


@dataclass(frozen=True)
class ScheduledBatch:
    """One submitted batch of source rows."""

    batch_index: int
    rows: list[RawProductRow]


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
    """Minimal reference result loading surface needed by a batch execution."""

    def load_many(self, rows: list[RawProductRow]) -> ResolvedReferenceResults: ...


class SupportsLegacyBackendRunner(Protocol):
    """Minimal backend surface needed by the reference result loader."""

    def run(
        self,
        backend_input_products: list[LegacyBackendInputProduct],
    ) -> list[ReferenceResult]: ...


class SupportsReferenceResultCache(Protocol):
    """Minimal cache surface needed by the reference result loader."""

    def load_many(self, codes: list[str]) -> dict[str, ReferenceResult]: ...

    def store_many(self, reference_results: list[ReferenceResult]) -> None: ...


class SupportsReferenceObserver(Protocol):
    """Reference observation strategy selected for one application run."""

    @property
    def requires_reference_results(self) -> bool: ...

    @property
    def parity_baselines(self) -> tuple[CheckParityBaseline, ...]: ...

    def observe_findings(
        self,
        reference_results: list[ReferenceResult],
    ) -> Iterable[ObservedFinding]: ...


class SupportsEnrichedSnapshotMaterializer(Protocol):
    """Projection surface for enriched runtime inputs."""

    def materialize(
        self,
        reference_results: list[ReferenceResult],
    ) -> list[EnrichedSnapshotResult]: ...


class SupportsReferenceFindingMaterializer(Protocol):
    """Projection surface for parity side observed findings."""

    def materialize(
        self,
        reference_results: list[ReferenceResult],
    ) -> Iterable[ObservedFinding]: ...


class SupportsReferenceRunner(Protocol):
    """Reference-side batch runtime used by the application batch loop."""

    def resolve(
        self,
        rows: list[RawProductRow],
    ) -> ResolvedReferenceBatch: ...


class SupportsMigratedRunner(Protocol):
    """Migrated-runtime batch executor used by the application batch loop."""

    def observe_findings(
        self,
        *,
        rows: list[RawProductRow],
        enriched_snapshots: list[EnrichedSnapshotResult],
    ) -> Iterable[ObservedFinding]: ...


class SupportsParityRunner(Protocol):
    """Strict-comparison runner used by the application batch loop."""

    def compare(
        self,
        *,
        product_count: int,
        reference_findings: Iterable[ObservedFinding],
        migrated_findings: Iterable[ObservedFinding],
    ) -> RunResult: ...


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


class SupportsRunAccumulator(Protocol):
    """Minimal accumulation surface needed by the batch loop."""

    def add_batch(self, batch_result: RunResult) -> None: ...


class SupportsRunRecorder(Protocol):
    """Minimal persistence surface used by the ordered batch merge path."""

    def record_batch(self, batch_result: BatchExecutionResult) -> None: ...

    def record_final_result(self, run_result: RunResult) -> None: ...
