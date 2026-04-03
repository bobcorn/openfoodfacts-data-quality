from __future__ import annotations

import logging
from concurrent.futures import Future
from pathlib import Path
from typing import TYPE_CHECKING, ParamSpec, Protocol, TypeVar

import app.application as application_module
import app.run.execution as execution_module
import app.run.orchestrator as orchestrator_module
import app.run.preparation as preparation_module
import app.run.runners as runners_module
import app.run.settings as settings_module
import pytest
from app.artifacts import (
    ApplicationArtifacts,
    display_path,
    prepare_application_artifacts,
)
from app.migration.catalog import MigrationCatalog
from app.reference.materializers import (
    EnrichedSnapshotMaterializer,
    ReferenceFindingMaterializer,
)
from app.reference.models import ReferenceResult
from app.reference.observers import NoReferenceObserver
from app.run.context_builders import check_context_builder_for
from app.run.execution import run_batches
from app.run.models import (
    BatchExecutionContext,
    BatchExecutionResult,
    BatchRunPlan,
    ExecutedApplicationRun,
    PreparedRun,
    ResolvedReferenceResults,
    RunSpec,
    ScheduledBatch,
)
from app.run.preparation import prepare_run
from app.run.profiles import ActiveCheckProfile
from app.run.runners import (
    LegacyReferenceRunner,
    MigratedRunner,
    NoReferenceRunner,
    ParityRunner,
)
from app.run.scheduler import BatchScheduler
from app.source.datasets import (
    ActiveDatasetProfile,
    SourceSelection,
    default_dataset_profile,
)

from openfoodfacts_data_quality.contracts.checks import LEGACY_PARITY_BASELINES
from openfoodfacts_data_quality.contracts.enrichment import EnrichedSnapshotResult

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from openfoodfacts_data_quality.checks.catalog import CheckCatalog
    from openfoodfacts_data_quality.contracts.checks import (
        CheckInputSurface,
        CheckParityBaseline,
    )
    from openfoodfacts_data_quality.contracts.context import NormalizedContext
    from openfoodfacts_data_quality.contracts.findings import Finding
    from openfoodfacts_data_quality.contracts.observations import ObservedFinding
    from openfoodfacts_data_quality.contracts.raw import RawProductRow
    from openfoodfacts_data_quality.contracts.run import RunResult

from openfoodfacts_data_quality.contracts.raw import RawProductRow


class RunResultFactory(Protocol):
    def __call__(self) -> RunResult: ...


_SubmitParams = ParamSpec("_SubmitParams")
_SubmitResult = TypeVar("_SubmitResult")


class _ImmediateExecutor:
    def submit(
        self,
        fn: Callable[_SubmitParams, _SubmitResult],
        /,
        *args: _SubmitParams.args,
        **kwargs: _SubmitParams.kwargs,
    ) -> Future[_SubmitResult]:
        future: Future[_SubmitResult] = Future()
        future.set_result(fn(*args, **kwargs))
        return future


class _ThreadPoolExecutorFactory:
    def __init__(self, **_: object) -> None:
        self.executor = _ImmediateExecutor()

    def __enter__(self) -> _ImmediateExecutor:
        return self.executor

    def __exit__(self, *_: object) -> None:
        return None


class _RecordingProgressReporter:
    heartbeat_interval_seconds = 0.0

    def __init__(self) -> None:
        self.heartbeats: list[tuple[int, int, int]] = []
        self.completed_batches: list[tuple[int, int]] = []

    def log_heartbeat(
        self,
        *,
        processed_products: int,
        buffered_results: tuple[BatchExecutionResult, ...],
        merged_batch_count: int,
        in_flight_count: int,
    ) -> None:
        self.heartbeats.append(
            (processed_products, merged_batch_count, in_flight_count)
        )
        assert buffered_results == ()

    def log_batch_completed(
        self,
        batch_result: BatchExecutionResult,
        *,
        processed_products: int,
    ) -> None:
        self.completed_batches.append((batch_result.batch_index, processed_products))


class _RecordingAccumulator:
    def __init__(self) -> None:
        self.run_results: list[RunResult] = []

    def add_batch(self, batch_result: RunResult) -> None:
        self.run_results.append(batch_result)


class _RecordingRunRecorder:
    def __init__(self) -> None:
        self.batch_indices: list[int] = []
        self.final_run_results: list[RunResult] = []

    def record_batch(self, batch_result: BatchExecutionResult) -> None:
        self.batch_indices.append(batch_result.batch_index)

    def record_final_result(self, run_result: RunResult) -> None:
        self.final_run_results.append(run_result)


class _UnusedCheckContextBuilder:
    requires_enriched_snapshots = False

    @property
    def input_surface(self) -> CheckInputSurface:
        return "raw_products"

    def build_contexts(
        self,
        *,
        rows: list[RawProductRow],
        enriched_snapshots: Sequence[EnrichedSnapshotResult],
    ) -> list[NormalizedContext]:
        raise AssertionError(
            f"Unexpected context build for rows={rows!r}, enriched_snapshots={enriched_snapshots!r}"
        )

    def iter_contexts(
        self,
        *,
        rows: list[RawProductRow],
        enriched_snapshots: Sequence[EnrichedSnapshotResult],
    ) -> Iterator[NormalizedContext]:
        raise AssertionError(
            f"Unexpected context iteration for rows={rows!r}, enriched_snapshots={enriched_snapshots!r}"
        )


class _RecordingReferenceResultLoader:
    def __init__(self, reference_results: list[ReferenceResult]) -> None:
        self.reference_results = reference_results
        self.calls: list[list[RawProductRow]] = []

    def load_many(
        self,
        rows: list[RawProductRow],
    ) -> ResolvedReferenceResults:
        self.calls.append(rows)
        return ResolvedReferenceResults(
            reference_results=self.reference_results,
            cache_hit_count=0,
            backend_run_count=len(rows),
        )


def _raw_row(**overrides: object) -> RawProductRow:
    return RawProductRow.model_validate({"code": "0000000000000", **overrides})


def _single_row_batch(
    *,
    batch_index: int = 1,
    code: str = "123",
    product_name: str = "Raw name",
) -> ScheduledBatch:
    return ScheduledBatch(
        batch_index=batch_index,
        rows=[_raw_row(code=code, product_name=product_name)],
    )


class _RecordingReferenceObserver:
    def __init__(
        self,
        findings: list[ObservedFinding] | None = None,
        *,
        requires_reference_results: bool,
    ) -> None:
        self.findings = findings or []
        self._requires_reference_results = requires_reference_results
        self.calls: list[list[ReferenceResult]] = []

    @property
    def requires_reference_results(self) -> bool:
        return self._requires_reference_results

    @property
    def parity_baselines(self) -> tuple[CheckParityBaseline, ...]:
        return ("legacy",) if self._requires_reference_results else ("none",)

    def observe_findings(
        self,
        reference_results: list[ReferenceResult],
    ) -> Sequence[ObservedFinding]:
        self.calls.append(reference_results)
        return self.findings


def _run_result_stub(
    expected_run_result: RunResult,
) -> Callable[..., RunResult]:
    def fake_evaluate_parity(*_: object, **__: object) -> RunResult:
        return expected_run_result

    return fake_evaluate_parity


def _execute_batch_with_stubbed_runtime(
    *,
    batch: ScheduledBatch,
    execution: BatchExecutionContext,
    expected_run_result: RunResult,
    monkeypatch: pytest.MonkeyPatch,
    observed_contexts: list[NormalizedContext] | None = None,
) -> BatchExecutionResult:
    def fake_iter_check_findings_with_evaluators(
        contexts: Sequence[NormalizedContext],
        *_: object,
        **__: object,
    ) -> Iterator[Finding]:
        if observed_contexts is not None:
            observed_contexts.extend(contexts)
        return iter(())

    monkeypatch.setattr(
        runners_module,
        "iter_check_findings_with_evaluators",
        fake_iter_check_findings_with_evaluators,
    )
    monkeypatch.setattr(
        runners_module,
        "evaluate_parity",
        _run_result_stub(expected_run_result),
    )
    return execution_module.execute_batch(batch, execution)


def _dummy_execution_context() -> BatchExecutionContext:
    return BatchExecutionContext(
        reference_runner=NoReferenceRunner(),
        migrated_runner=MigratedRunner(
            check_context_builder=_UnusedCheckContextBuilder(),
            evaluators={},
        ),
        parity_runner=ParityRunner(
            run_id="run",
            source_snapshot_id="source-snapshot",
            active_checks=(),
        ),
    )


def _execution_context(
    *,
    input_surface: CheckInputSurface,
    reference_result_loader: _RecordingReferenceResultLoader | None,
    reference_observer: _RecordingReferenceObserver,
    include_enriched_snapshots: bool,
) -> BatchExecutionContext:
    return BatchExecutionContext(
        reference_runner=(
            LegacyReferenceRunner(
                reference_result_loader=reference_result_loader,
                enriched_snapshot_materializer=(
                    EnrichedSnapshotMaterializer()
                    if include_enriched_snapshots
                    else None
                ),
                reference_finding_materializer=(
                    ReferenceFindingMaterializer(reference_observer)
                    if reference_observer.requires_reference_results
                    else None
                ),
            )
            if reference_result_loader is not None
            else NoReferenceRunner()
        ),
        migrated_runner=MigratedRunner(
            check_context_builder=check_context_builder_for(input_surface),
            evaluators={},
        ),
        parity_runner=ParityRunner(
            run_id="run",
            source_snapshot_id="source-snapshot",
            active_checks=(),
        ),
    )


def _batch_execution_result(
    batch: ScheduledBatch,
    run_result: RunResult,
) -> BatchExecutionResult:
    return BatchExecutionResult(
        batch_index=batch.batch_index,
        row_count=len(batch.rows),
        cache_hit_count=0,
        backend_run_count=0,
        reference_finding_count=0,
        migrated_finding_count=0,
        run_result=run_result,
        elapsed_seconds=0.1,
    )


def test_prepare_application_artifacts_recreates_latest_tree(tmp_path: Path) -> None:
    stale_file = tmp_path / "artifacts" / "latest" / "stale.txt"
    stale_file.parent.mkdir(parents=True)
    stale_file.write_text("stale", encoding="utf-8")

    artifacts = prepare_application_artifacts(tmp_path)

    assert artifacts.artifacts_dir == tmp_path / "artifacts" / "latest"
    assert artifacts.site_dir == artifacts.artifacts_dir / "site"
    assert artifacts.legacy_backend_stderr_path == (
        artifacts.artifacts_dir / "legacy-backend-stderr.log"
    )
    assert artifacts.site_dir.is_dir()
    assert not stale_file.exists()


def test_display_path_prefers_relative_paths() -> None:
    project_root = Path(__file__).resolve().parents[1]
    inside_project = project_root / "artifacts" / "latest" / "site"
    outside_project = Path("/tmp/outside")

    assert display_path(inside_project, project_root) == "artifacts/latest/site"
    assert display_path(outside_project, project_root) == "/tmp/outside"


def test_prepare_run_collects_profile_and_definition_counts(
    monkeypatch: pytest.MonkeyPatch,
    default_check_catalog: CheckCatalog,
    tmp_path: Path,
) -> None:
    python_check = next(
        check
        for check in default_check_catalog.checks
        if check.definition_language == "python"
        and check.supports_input_surface("raw_products")
        and check.parity_baseline == "legacy"
    )
    dsl_check = next(
        check
        for check in default_check_catalog.checks
        if check.definition_language == "dsl"
        and check.supports_input_surface("raw_products")
        and check.parity_baseline == "legacy"
    )
    active_profile = ActiveCheckProfile(
        name="raw_products",
        description="Active test profile",
        check_input_surface="raw_products",
        parity_baselines=LEGACY_PARITY_BASELINES,
        jurisdictions=None,
        check_ids=(python_check.id, dsl_check.id),
        checks=(python_check, dsl_check),
    )
    active_dataset_profile = ActiveDatasetProfile(
        name="smoke",
        description="Fast deterministic sample",
        selection=SourceSelection(
            kind="stable_sample",
            sample_size=10,
            seed=99,
        ),
    )

    def fake_source_snapshot_id_for(_: Path) -> str:
        return "snapshot-123"

    def fake_count_source_rows(
        _: Path, *, selection: SourceSelection | None = None
    ) -> int:
        assert selection == active_dataset_profile.selection
        return 42

    def fake_get_default_check_catalog() -> CheckCatalog:
        return default_check_catalog

    def fake_load_dataset_profile(
        config_path: Path,
        profile_name: str | None = None,
    ) -> ActiveDatasetProfile:
        assert config_path == tmp_path / "config" / "dataset-profiles.toml"
        assert profile_name == "smoke"
        return active_dataset_profile

    loaded_migration_catalog = MigrationCatalog()

    def fake_load_check_profile(
        config_path: Path,
        profile_name: str | None = None,
        *,
        catalog: CheckCatalog | None = None,
        migration_catalog: MigrationCatalog | None = None,
    ) -> ActiveCheckProfile:
        assert config_path == tmp_path / "config" / "check-profiles.toml"
        assert profile_name == "ignored"
        assert catalog is default_check_catalog
        assert migration_catalog is loaded_migration_catalog
        return active_profile

    monkeypatch.setattr(
        preparation_module,
        "source_snapshot_id_for",
        fake_source_snapshot_id_for,
    )
    monkeypatch.setattr(
        preparation_module,
        "count_source_rows",
        fake_count_source_rows,
    )
    monkeypatch.setattr(
        preparation_module,
        "load_dataset_profile",
        fake_load_dataset_profile,
    )
    monkeypatch.setattr(
        preparation_module,
        "get_default_check_catalog",
        fake_get_default_check_catalog,
    )
    monkeypatch.setattr(
        preparation_module,
        "load_check_profile",
        fake_load_check_profile,
    )

    def fake_load_migration_catalog(**_: object) -> MigrationCatalog:
        return loaded_migration_catalog

    monkeypatch.setattr(
        preparation_module,
        "load_migration_catalog",
        fake_load_migration_catalog,
    )

    prepared = prepare_run(
        RunSpec(
            project_root=tmp_path,
            db_path=tmp_path / "data" / "products.duckdb",
            batch_size=100,
            mismatch_examples_limit=5,
            batch_workers=2,
            legacy_backend_workers=1,
            reference_result_cache_dir=tmp_path / "cache",
            reference_result_cache_salt="salt",
            check_profile_name="ignored",
            dataset_profile_name="smoke",
        ),
        logger=logging.getLogger("test-run-flow"),
    )

    assert prepared.source_snapshot_id == "snapshot-123"
    assert prepared.run_id
    assert prepared.product_count == 42
    assert prepared.active_check_profile == active_profile
    assert prepared.check_context_builder.input_surface == "raw_products"
    assert set(prepared.evaluators) == {python_check.id, dsl_check.id}
    assert prepared.python_count == 1
    assert prepared.dsl_count == 1
    assert prepared.legacy_parity_count == sum(
        1 for check in active_profile.checks if check.parity_baseline == "legacy"
    )
    assert prepared.runtime_only_count == sum(
        1 for check in active_profile.checks if check.parity_baseline == "none"
    )
    assert prepared.active_dataset_profile == active_dataset_profile
    assert prepared.active_migration_plan.family_count == 0
    assert prepared.requires_enriched_snapshots is False
    assert prepared.requires_reference_findings is True
    assert prepared.requires_reference_results is True


def test_configured_source_snapshot_path_requires_explicit_source_snapshot_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("SOURCE_SNAPSHOT_PATH", raising=False)

    with pytest.raises(
        ValueError,
        match="SOURCE_SNAPSHOT_PATH must be set for local runtime runs",
    ):
        settings_module.configured_source_snapshot_path(tmp_path)


def test_configured_run_spec_requires_explicit_source_snapshot_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("SOURCE_SNAPSHOT_PATH", raising=False)

    with pytest.raises(
        ValueError,
        match="SOURCE_SNAPSHOT_PATH must be set for local runtime runs",
    ):
        settings_module.configured_run_spec(tmp_path)


def test_configured_run_spec_collects_runtime_configuration(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "data" / "products.duckdb"
    parity_store_path = tmp_path / "stores" / "parity.duckdb"
    expected_differences_path = tmp_path / "config" / "expected-differences.toml"
    monkeypatch.setenv("SOURCE_SNAPSHOT_PATH", str(db_path))
    monkeypatch.setenv("BATCH_SIZE", "17")
    monkeypatch.setenv("MISMATCH_EXAMPLES_LIMIT", "9")
    monkeypatch.setenv("BATCH_WORKERS", "3")
    monkeypatch.setenv("LEGACY_BACKEND_WORKERS", "2")
    monkeypatch.setenv("CHECK_PROFILE", "focused")
    monkeypatch.setenv("SOURCE_DATASET_PROFILE", "smoke")
    monkeypatch.setenv("PARITY_STORE_PATH", str(parity_store_path))
    monkeypatch.setenv(
        "PARITY_EXPECTED_DIFFERENCES_PATH",
        str(expected_differences_path),
    )
    monkeypatch.setenv(
        "MIGRATION_INVENTORY_PATH",
        str(tmp_path / "legacy" / "legacy_families.json"),
    )
    monkeypatch.setenv(
        "MIGRATION_ESTIMATION_SHEET_PATH",
        str(tmp_path / "legacy" / "estimation_sheet.csv"),
    )

    run_spec = settings_module.configured_run_spec(tmp_path)

    assert run_spec.project_root == tmp_path.resolve()
    assert run_spec.db_path == db_path.resolve()
    assert run_spec.batch_size == 17
    assert run_spec.mismatch_examples_limit == 9
    assert run_spec.batch_workers == 3
    assert run_spec.legacy_backend_workers == 2
    assert run_spec.check_profile_name == "focused"
    assert run_spec.dataset_profile_name == "smoke"
    assert run_spec.parity_store_path == parity_store_path.resolve()
    assert run_spec.expected_differences_path == expected_differences_path.resolve()
    assert (
        run_spec.legacy_inventory_artifact_path
        == (tmp_path / "legacy" / "legacy_families.json").resolve()
    )
    assert (
        run_spec.legacy_estimation_sheet_path
        == (tmp_path / "legacy" / "estimation_sheet.csv").resolve()
    )
    assert (
        run_spec.reference_result_cache_dir
        == (tmp_path / "data" / "reference_result_cache").resolve()
    )


def test_configured_parity_expected_differences_path_supports_explicit_disable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("PARITY_EXPECTED_DIFFERENCES_PATH", "   ")

    assert settings_module.configured_parity_expected_differences_path(tmp_path) is None


def test_configured_check_profile_name_normalizes_blank_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CHECK_PROFILE", raising=False)
    assert settings_module.configured_check_profile_name() is None

    monkeypatch.setenv("CHECK_PROFILE", "   ")
    assert settings_module.configured_check_profile_name() is None

    monkeypatch.setenv("CHECK_PROFILE", " focused ")
    assert settings_module.configured_check_profile_name() == "focused"


def test_configured_source_dataset_profile_name_normalizes_blank_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SOURCE_DATASET_PROFILE", raising=False)
    assert settings_module.configured_source_dataset_profile_name() is None

    monkeypatch.setenv("SOURCE_DATASET_PROFILE", "   ")
    assert settings_module.configured_source_dataset_profile_name() is None

    monkeypatch.setenv("SOURCE_DATASET_PROFILE", " smoke ")
    assert settings_module.configured_source_dataset_profile_name() == "smoke"


def test_build_site_requires_existing_source_snapshot_path(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Source DuckDB not found"):
        application_module.build_site(
            RunSpec(
                project_root=tmp_path,
                db_path=tmp_path / "data" / "missing.duckdb",
                batch_size=100,
                mismatch_examples_limit=5,
                batch_workers=2,
                legacy_backend_workers=1,
                reference_result_cache_dir=tmp_path / "cache",
                reference_result_cache_salt="salt",
            )
        )


def test_application_site_builder_prefers_store_backed_rendering(
    monkeypatch: pytest.MonkeyPatch,
    run_result_factory: RunResultFactory,
    tmp_path: Path,
) -> None:
    site_dir = tmp_path / "artifacts" / "latest" / "site"
    render_calls: list[tuple[str, Path]] = []

    def fake_execute(
        _: orchestrator_module.ApplicationRunner,
    ) -> ExecutedApplicationRun:
        return ExecutedApplicationRun(
            run_result=run_result_factory(),
            artifacts=ApplicationArtifacts(
                artifacts_dir=tmp_path / "artifacts" / "latest",
                site_dir=site_dir,
                legacy_backend_stderr_path=(
                    tmp_path / "artifacts" / "latest" / "legacy-backend-stderr.log"
                ),
            ),
        )

    def fake_render_report_from_store(
        *,
        store_path: Path,
        run_id: str,
        output_dir: Path,
        legacy_source_root: Path | None = None,
    ) -> None:
        del store_path, run_id, legacy_source_root
        render_calls.append(("store", output_dir))

    def unexpected_render_report(*_: object, **__: object) -> None:
        raise AssertionError(
            "in-memory renderer should not be used when a store exists"
        )

    monkeypatch.setattr(orchestrator_module.ApplicationRunner, "execute", fake_execute)
    monkeypatch.setattr(
        application_module,
        "render_report_from_store",
        fake_render_report_from_store,
    )
    monkeypatch.setattr(
        application_module,
        "render_report",
        unexpected_render_report,
    )

    site_path = application_module.ApplicationSiteBuilder(
        RunSpec(
            project_root=tmp_path,
            db_path=tmp_path / "data" / "products.duckdb",
            batch_size=100,
            mismatch_examples_limit=5,
            batch_workers=2,
            legacy_backend_workers=1,
            reference_result_cache_dir=tmp_path / "cache",
            reference_result_cache_salt="salt",
            parity_store_path=tmp_path / "parity-store.duckdb",
        )
    ).build()

    assert site_path == site_dir
    assert render_calls == [("store", site_dir)]


def test_application_site_builder_uses_in_memory_rendering_without_store(
    monkeypatch: pytest.MonkeyPatch,
    run_result_factory: RunResultFactory,
    tmp_path: Path,
) -> None:
    site_dir = tmp_path / "artifacts" / "latest" / "site"
    run_result = run_result_factory()
    render_calls: list[tuple[object, Path]] = []

    def fake_execute(
        _: orchestrator_module.ApplicationRunner,
    ) -> ExecutedApplicationRun:
        return ExecutedApplicationRun(
            run_result=run_result,
            artifacts=ApplicationArtifacts(
                artifacts_dir=tmp_path / "artifacts" / "latest",
                site_dir=site_dir,
                legacy_backend_stderr_path=(
                    tmp_path / "artifacts" / "latest" / "legacy-backend-stderr.log"
                ),
            ),
        )

    def fake_render_report(
        resolved_run_result: object,
        output_dir: Path,
        *,
        legacy_source_root: Path | None = None,
    ) -> None:
        del legacy_source_root
        render_calls.append((resolved_run_result, output_dir))

    def unexpected_render_report_from_store(*_: object, **__: object) -> None:
        raise AssertionError("store-backed renderer should not be used without a store")

    monkeypatch.setattr(orchestrator_module.ApplicationRunner, "execute", fake_execute)
    monkeypatch.setattr(
        application_module,
        "render_report",
        fake_render_report,
    )
    monkeypatch.setattr(
        application_module,
        "render_report_from_store",
        unexpected_render_report_from_store,
    )

    site_path = application_module.ApplicationSiteBuilder(
        RunSpec(
            project_root=tmp_path,
            db_path=tmp_path / "data" / "products.duckdb",
            batch_size=100,
            mismatch_examples_limit=5,
            batch_workers=2,
            legacy_backend_workers=1,
            reference_result_cache_dir=tmp_path / "cache",
            reference_result_cache_salt="salt",
        )
    ).build()

    assert site_path == site_dir
    assert render_calls == [(run_result, site_dir)]


def test_warns_when_legacy_backend_workers_exceed_batch_workers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("test-run-flow")
    caplog.set_level(logging.WARNING, logger=logger.name)

    orchestrator_module.warn_if_legacy_backend_workers_exceed_batch_workers(
        requires_reference_results=True,
        batch_workers=1,
        legacy_backend_workers=2,
        logger=logger,
    )

    assert "LEGACY_BACKEND_WORKERS=2 exceeds BATCH_WORKERS=1" in caplog.text


def test_does_not_warn_about_backend_workers_without_reference_results(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("test-run-flow")
    caplog.set_level(logging.WARNING, logger=logger.name)

    orchestrator_module.warn_if_legacy_backend_workers_exceed_batch_workers(
        requires_reference_results=False,
        batch_workers=1,
        legacy_backend_workers=2,
        logger=logger,
    )

    assert caplog.text == ""


def test_with_reference_result_cache_replaces_cache_location(
    default_check_catalog: CheckCatalog,
    tmp_path: Path,
) -> None:
    sample_check = default_check_catalog.checks[0]
    prepared = PreparedRun(
        source_snapshot_id="snapshot-123",
        run_id="run-123",
        product_count=1,
        active_check_profile=ActiveCheckProfile(
            name="enriched_products",
            description="Active test profile",
            check_input_surface="enriched_products",
            parity_baselines=LEGACY_PARITY_BASELINES,
            jurisdictions=None,
            check_ids=(sample_check.id,),
            checks=(sample_check,),
        ),
        check_context_builder=check_context_builder_for("enriched_products"),
        reference_observer=NoReferenceObserver(),
        evaluators={
            sample_check.id: default_check_catalog.evaluators_by_id[sample_check.id]
        },
        reference_result_cache_key=None,
        reference_result_cache_path=None,
        python_count=1 if sample_check.definition_language == "python" else 0,
        dsl_count=1 if sample_check.definition_language == "dsl" else 0,
        legacy_parity_count=1 if sample_check.parity_baseline == "legacy" else 0,
        runtime_only_count=1 if sample_check.parity_baseline == "none" else 0,
    )

    updated = prepared.with_reference_result_cache(
        result_cache_key="cache-key",
        result_cache_path=tmp_path / "reference-result-cache.duckdb",
    )

    assert updated.reference_result_cache_key == "cache-key"
    assert (
        updated.reference_result_cache_path
        == tmp_path / "reference-result-cache.duckdb"
    )
    assert updated.source_snapshot_id == prepared.source_snapshot_id
    assert updated.active_check_profile == prepared.active_check_profile
    assert updated.check_context_builder is prepared.check_context_builder


def test_batch_scheduler_submits_and_merges_batches_in_order(
    run_result_factory: RunResultFactory,
) -> None:
    scheduler = BatchScheduler(
        batch_iterator=iter(
            [
                ScheduledBatch(batch_index=1, rows=[_raw_row(code="1")]),
                ScheduledBatch(batch_index=2, rows=[_raw_row(code="2")]),
            ]
        ),
        executor=_ImmediateExecutor(),
        process_batch=lambda batch: _batch_execution_result(
            batch,
            run_result_factory(),
        ),
        worker_limit=2,
    )

    scheduler.submit_ready_batches()
    completed = scheduler.wait_for_completed_batches(timeout_seconds=0.0)
    scheduler.record_completed_batches(completed)

    merged = scheduler.merge_ready_batches()

    assert [result.batch_index for result in merged] == [1, 2]
    assert scheduler.buffered_results() == ()

    scheduler.submit_ready_batches()
    assert scheduler.has_pending_work() is False


def test_run_batches_processes_all_batches(
    monkeypatch: pytest.MonkeyPatch,
    run_result_factory: RunResultFactory,
    tmp_path: Path,
) -> None:
    def fake_iter_source_batches(
        _: Path, *, batch_size: int, selection: SourceSelection
    ) -> list[list[RawProductRow]]:
        assert batch_size == 2
        assert selection == default_dataset_profile().selection
        return [
            [_raw_row(code="1")],
            [_raw_row(code="2"), _raw_row(code="3")],
        ]

    def fake_execute_batch(
        batch: ScheduledBatch,
        _: BatchExecutionContext,
    ) -> BatchExecutionResult:
        return _batch_execution_result(batch, run_result_factory())

    monkeypatch.setattr(
        execution_module,
        "ThreadPoolExecutor",
        _ThreadPoolExecutorFactory,
    )
    monkeypatch.setattr(
        execution_module,
        "iter_source_batches",
        fake_iter_source_batches,
    )
    monkeypatch.setattr(execution_module, "execute_batch", fake_execute_batch)
    progress = _RecordingProgressReporter()
    accumulator = _RecordingAccumulator()
    recorder = _RecordingRunRecorder()

    run_batches(
        plan=BatchRunPlan(
            db_path=tmp_path / "products.duckdb",
            batch_size=2,
            batch_workers=2,
            legacy_backend_workers=2,
            source_selection=default_dataset_profile().selection,
        ),
        execution=_dummy_execution_context(),
        execution_progress=progress,
        accumulator=accumulator,
        run_recorder=recorder,
    )

    assert progress.completed_batches == [(1, 1), (2, 3)]
    assert progress.heartbeats == []
    assert len(accumulator.run_results) == 2
    assert recorder.batch_indices == [1, 2]


@pytest.mark.parametrize(
    (
        "input_surface",
        "expected_product_name",
        "expected_lang",
        "expects_reference_results",
    ),
    [
        ("raw_products", "Raw name", None, False),
        ("enriched_products", "Enriched name", "fr", True),
    ],
)
def test_execute_batch_uses_selected_context_builder(
    input_surface: CheckInputSurface,
    expected_product_name: str,
    expected_lang: str | None,
    expects_reference_results: bool,
    monkeypatch: pytest.MonkeyPatch,
    run_result_factory: RunResultFactory,
    reference_result_factory: Callable[..., ReferenceResult],
) -> None:
    batch = _single_row_batch()
    reference_result = reference_result_factory(
        code="123",
        enriched_snapshot={
            "product": {
                "code": "123",
                "product_name": "Enriched name",
                "lang": "fr",
            }
        },
    )
    reference_result_loader = (
        _RecordingReferenceResultLoader([reference_result])
        if expects_reference_results
        else None
    )
    reference_observer = _RecordingReferenceObserver(requires_reference_results=False)
    observed_contexts: list[NormalizedContext] = []
    expected_run_result = run_result_factory()
    result = _execute_batch_with_stubbed_runtime(
        batch=batch,
        execution=_execution_context(
            input_surface=input_surface,
            reference_result_loader=reference_result_loader,
            reference_observer=reference_observer,
            include_enriched_snapshots=True,
        ),
        expected_run_result=expected_run_result,
        monkeypatch=monkeypatch,
        observed_contexts=observed_contexts,
    )

    if reference_result_loader is None:
        assert reference_observer.calls == []
    else:
        assert [
            [product.code for product in call] for call in reference_result_loader.calls
        ] == [["123"]]
    assert reference_observer.calls == []
    assert [context.product.product_name for context in observed_contexts] == [
        expected_product_name
    ]
    assert observed_contexts[0].product.lang == expected_lang
    assert result.run_result == expected_run_result


def test_execute_batch_loads_reference_results_for_legacy_parity_without_enriched_contexts(
    monkeypatch: pytest.MonkeyPatch,
    run_result_factory: RunResultFactory,
    reference_result_factory: Callable[..., ReferenceResult],
) -> None:
    batch = _single_row_batch()
    reference_result_loader = _RecordingReferenceResultLoader(
        [reference_result_factory(code="123")]
    )
    reference_observer = _RecordingReferenceObserver(requires_reference_results=True)
    expected_run_result = run_result_factory()
    result = _execute_batch_with_stubbed_runtime(
        batch=batch,
        execution=_execution_context(
            input_surface="raw_products",
            reference_result_loader=reference_result_loader,
            reference_observer=reference_observer,
            include_enriched_snapshots=False,
        ),
        expected_run_result=expected_run_result,
        monkeypatch=monkeypatch,
    )

    assert [
        [product.code for product in call] for call in reference_result_loader.calls
    ] == [["123"]]
    assert [result.code for result in reference_observer.calls[0]] == ["123"]
    assert result.run_result == expected_run_result


def test_execute_batch_resolves_enrichment_and_reference_from_one_batch_input_loader(
    monkeypatch: pytest.MonkeyPatch,
    run_result_factory: RunResultFactory,
    reference_result_factory: Callable[..., ReferenceResult],
    observed_finding_factory: Callable[..., ObservedFinding],
) -> None:
    batch = _single_row_batch()
    reference_result_loader = _RecordingReferenceResultLoader(
        [
            reference_result_factory(
                code="123",
                enriched_snapshot={
                    "product": {
                        "code": "123",
                        "product_name": "Enriched name",
                    }
                },
            )
        ]
    )
    reference_observer = _RecordingReferenceObserver(
        findings=[
            observed_finding_factory(
                check_id="en:quantity-not-recognized",
                product_id="123",
                severity="warning",
                side="reference",
            )
        ],
        requires_reference_results=True,
    )
    observed_contexts: list[NormalizedContext] = []
    expected_run_result = run_result_factory().model_copy(update={"reference_total": 1})
    result = _execute_batch_with_stubbed_runtime(
        batch=batch,
        execution=_execution_context(
            input_surface="enriched_products",
            reference_result_loader=reference_result_loader,
            reference_observer=reference_observer,
            include_enriched_snapshots=True,
        ),
        expected_run_result=expected_run_result,
        monkeypatch=monkeypatch,
        observed_contexts=observed_contexts,
    )

    assert [
        [product.code for product in call] for call in reference_result_loader.calls
    ] == [["123"]]
    assert [
        reference_result.code for reference_result in reference_observer.calls[0]
    ] == ["123"]
    assert [context.product.product_name for context in observed_contexts] == [
        "Enriched name"
    ]
    assert result.reference_finding_count == 1
