from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, ParamSpec, Protocol

from migration.source.datasets import (
    ActiveDatasetProfile,
    SourceSelection,
    default_dataset_profile,
)
from migration.source.models import SourceInputSummary

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from migration.artifacts import ArtifactWorkspace
    from migration.reference.models import ReferenceResult
    from migration.run.context_builders import SupportsCheckContextBuilder
    from migration.run.profiles import ActiveCheckProfile
    from migration.source.models import (
        ProductDocument,
        SourceBatchRecord,
    )
    from off_data_quality.contracts.checks import CheckParityBaseline
    from off_data_quality.contracts.context import CheckContext
    from off_data_quality.contracts.observations import ObservedFinding
    from off_data_quality.contracts.run import RunResult
    from off_data_quality.execution import CheckEvaluator

_SubmitParams = ParamSpec("_SubmitParams")


@dataclass(frozen=True)
class BatchStageTimings:
    """Per-batch execution timings for the main runtime stages."""

    source_read_seconds: float = 0.0
    reference_load_seconds: float = 0.0
    reference_check_context_materialization_seconds: float = 0.0
    reference_finding_materialization_seconds: float = 0.0
    migrated_findings_seconds: float = 0.0
    parity_compare_seconds: float = 0.0

    def as_mapping(self) -> dict[str, float]:
        """Return a stable mapping view for reporting and benchmark output."""
        return {
            "source_read_seconds": self.source_read_seconds,
            "reference_load_seconds": self.reference_load_seconds,
            "reference_check_context_materialization_seconds": (
                self.reference_check_context_materialization_seconds
            ),
            "reference_finding_materialization_seconds": (
                self.reference_finding_materialization_seconds
            ),
            "migrated_findings_seconds": self.migrated_findings_seconds,
            "parity_compare_seconds": self.parity_compare_seconds,
        }


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
    stage_timings: BatchStageTimings = field(default_factory=BatchStageTimings)


@dataclass(frozen=True)
class ResolvedReferenceResults:
    """Ordered reference results together with execution telemetry."""

    reference_results: list[ReferenceResult]
    cache_hit_count: int
    backend_run_count: int
    load_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class ResolvedReferenceBatch:
    """Reference-side batch inputs after cache/backend loading and projection."""

    reference_check_contexts: list[CheckContext]
    reference_findings: tuple[ObservedFinding, ...]
    cache_hit_count: int
    backend_run_count: int
    load_seconds: float = 0.0
    reference_check_context_materialization_seconds: float = 0.0
    reference_finding_materialization_seconds: float = 0.0


@dataclass(frozen=True)
class RunPreparationTimings:
    """Measured timings for the pre-batch run preparation stage."""

    prepare_run_seconds: float = 0.0
    source_snapshot_id_seconds: float = 0.0
    dataset_profile_load_seconds: float = 0.0
    source_row_count_seconds: float = 0.0

    def as_mapping(self) -> dict[str, float]:
        """Return a stable mapping view for store persistence and benchmark output."""
        return {
            "prepare_run_seconds": self.prepare_run_seconds,
            "source_snapshot_id_seconds": self.source_snapshot_id_seconds,
            "dataset_profile_load_seconds": self.dataset_profile_load_seconds,
            "source_row_count_seconds": self.source_row_count_seconds,
        }


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
    source_input_summary: SourceInputSummary = field(default_factory=SourceInputSummary)
    preparation_timings: RunPreparationTimings = field(
        default_factory=RunPreparationTimings
    )
    active_dataset_profile: ActiveDatasetProfile = field(
        default_factory=default_dataset_profile
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
            source_input_summary=self.source_input_summary,
            preparation_timings=self.preparation_timings,
            active_dataset_profile=self.active_dataset_profile,
        )

    @property
    def requires_reference_check_contexts(self) -> bool:
        """Return whether migrated checks need reference-side enriched contexts."""
        return self.check_context_builder.requires_reference_check_contexts

    @property
    def requires_reference_findings(self) -> bool:
        """Return whether strict comparison needs reference findings for this run."""
        return self.reference_observer.requires_reference_results

    @property
    def requires_reference_results(self) -> bool:
        """Return whether this run needs reference results at all."""
        return (
            self.requires_reference_check_contexts or self.requires_reference_findings
        )


@dataclass(frozen=True, slots=True)
class ExecutedMigrationRun:
    """Completed migration execution plus the prepared artifact workspace."""

    run_result: RunResult
    artifact_workspace: ArtifactWorkspace
    source_input_summary: SourceInputSummary = field(default_factory=SourceInputSummary)


@dataclass(frozen=True, slots=True)
class RunSpec:
    """Explicit migration run specification resolved before orchestration starts."""

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
    """Static batch-loop configuration for one migration run."""

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
    """One submitted batch of source documents."""

    batch_index: int
    records: list[SourceBatchRecord]
    source_read_seconds: float = 0.0

    @property
    def product_documents(self) -> list[ProductDocument]:
        """Return the selected product documents for the reference path."""
        return [record.product_document for record in self.records]


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

    def load_many(
        self,
        product_documents: list[ProductDocument],
    ) -> ResolvedReferenceResults: ...


class SupportsLegacyBackendRunner(Protocol):
    """Minimal backend surface needed by the reference result loader."""

    def run(
        self,
        backend_input_payloads: Sequence[Mapping[str, object]],
    ) -> list[ReferenceResult]: ...


class SupportsReferenceResultCache(Protocol):
    """Minimal cache surface needed by the reference result loader."""

    def load_many(self, codes: list[str]) -> dict[str, ReferenceResult]: ...

    def store_many(self, reference_results: list[ReferenceResult]) -> None: ...


class SupportsReferenceObserver(Protocol):
    """Reference observation strategy selected for one migration run."""

    @property
    def requires_reference_results(self) -> bool: ...

    @property
    def parity_baselines(self) -> tuple[CheckParityBaseline, ...]: ...

    def observe_findings(
        self,
        reference_results: list[ReferenceResult],
    ) -> Iterable[ObservedFinding]: ...


class SupportsReferenceCheckContextMaterializer(Protocol):
    """Projection surface for reference-side enriched check contexts."""

    def materialize(
        self,
        reference_results: list[ReferenceResult],
    ) -> list[CheckContext]: ...


class SupportsReferenceFindingMaterializer(Protocol):
    """Projection surface for parity side observed findings."""

    def materialize(
        self,
        reference_results: list[ReferenceResult],
    ) -> Iterable[ObservedFinding]: ...


class SupportsReferenceRunner(Protocol):
    """Reference-side batch runtime used by the migration batch loop."""

    def resolve(
        self,
        product_documents: list[ProductDocument],
    ) -> ResolvedReferenceBatch: ...


class SupportsMigratedRunner(Protocol):
    """Migrated-runtime batch executor used by the migration batch loop."""

    def observe_findings(
        self,
        *,
        reference_check_contexts: list[CheckContext],
    ) -> Iterable[ObservedFinding]: ...


class SupportsParityRunner(Protocol):
    """Strict-comparison runner used by the migration batch loop."""

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
