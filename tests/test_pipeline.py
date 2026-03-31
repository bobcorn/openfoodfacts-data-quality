from __future__ import annotations

import logging
from concurrent.futures import Future
from pathlib import Path
from typing import TYPE_CHECKING, Any, ParamSpec, Protocol, TypeVar

import app.pipeline.execution as execution_module
import app.pipeline.preparation as preparation_module
import pytest
from app.legacy_backend.input_projection import LegacyBackendInputProduct
from app.pipeline.batch_inputs import (
    NoOpBatchInputResolver,
    ReferenceBatchInputResolver,
)
from app.pipeline.context_builders import check_context_builder_for
from app.pipeline.execution import run_batches
from app.pipeline.models import (
    BatchExecutionContext,
    BatchExecutionResult,
    BatchRunPlan,
    PreparedRun,
    ResolvedReferenceResults,
    ScheduledBatch,
)
from app.pipeline.preparation import (
    display_path,
    prepare_artifacts_dir,
    prepare_run,
)
from app.pipeline.profiles import ActiveCheckProfile
from app.pipeline.scheduler import BatchScheduler
from app.reference.models import ReferenceResult
from app.reference.observers import NoReferenceObserver

from openfoodfacts_data_quality.contracts.checks import LEGACY_PARITY_BASELINES
from openfoodfacts_data_quality.contracts.enrichment import EnrichedSnapshotResult

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from app.parity.models import ObservedFinding, ParityResult

    from openfoodfacts_data_quality.checks.catalog import CheckCatalog
    from openfoodfacts_data_quality.contracts.checks import (
        CheckInputSurface,
        CheckParityBaseline,
    )
    from openfoodfacts_data_quality.contracts.context import NormalizedContext
    from openfoodfacts_data_quality.contracts.findings import Finding


class ParityResultFactory(Protocol):
    def __call__(self) -> ParityResult: ...


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
        self.parity_results: list[ParityResult] = []

    def add_batch(self, batch_result: ParityResult) -> None:
        self.parity_results.append(batch_result)


class _UnusedCheckContextBuilder:
    requires_enriched_snapshots = False

    @property
    def input_surface(self) -> CheckInputSurface:
        return "raw_products"

    def build_contexts(
        self,
        *,
        rows: list[dict[str, Any]],
        enriched_snapshots: Sequence[EnrichedSnapshotResult],
    ) -> list[NormalizedContext]:
        raise AssertionError(
            f"Unexpected context build for rows={rows!r}, enriched_snapshots={enriched_snapshots!r}"
        )


class _RecordingReferenceResultLoader:
    def __init__(self, reference_results: list[ReferenceResult]) -> None:
        self.reference_results = reference_results
        self.calls: list[list[LegacyBackendInputProduct]] = []

    def load_many(
        self,
        backend_input_products: list[LegacyBackendInputProduct],
    ) -> ResolvedReferenceResults:
        self.calls.append(backend_input_products)
        return ResolvedReferenceResults(
            reference_results=self.reference_results,
            cache_hit_count=0,
            backend_run_count=len(backend_input_products),
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
    ) -> list[ObservedFinding]:
        self.calls.append(reference_results)
        return self.findings


def _parity_result_stub(
    expected_parity: ParityResult,
) -> Callable[..., ParityResult]:
    def fake_evaluate_parity(*_: object, **__: object) -> ParityResult:
        return expected_parity

    return fake_evaluate_parity


def _execute_batch_with_stubbed_runtime(
    *,
    batch: ScheduledBatch,
    execution: BatchExecutionContext,
    expected_parity: ParityResult,
    monkeypatch: pytest.MonkeyPatch,
    observed_contexts: list[NormalizedContext] | None = None,
) -> BatchExecutionResult:
    def fake_run_checks_with_evaluators(
        contexts: list[NormalizedContext],
        *_: object,
        **__: object,
    ) -> list[Finding]:
        if observed_contexts is not None:
            observed_contexts.extend(contexts)
        return []

    monkeypatch.setattr(
        execution_module,
        "run_checks_with_evaluators",
        fake_run_checks_with_evaluators,
    )
    monkeypatch.setattr(
        execution_module,
        "evaluate_parity",
        _parity_result_stub(expected_parity),
    )
    return execution_module.execute_batch(batch, execution)


def _dummy_execution_context() -> BatchExecutionContext:
    return BatchExecutionContext(
        batch_input_resolver=NoOpBatchInputResolver(),
        evaluators={},
        active_checks=(),
        check_context_builder=_UnusedCheckContextBuilder(),
        run_id="run",
        source_snapshot_id="source-snapshot",
    )


def _batch_execution_result(
    batch: ScheduledBatch,
    parity_result: ParityResult,
) -> BatchExecutionResult:
    return BatchExecutionResult(
        batch_index=batch.batch_index,
        row_count=len(batch.rows),
        cache_hit_count=0,
        backend_run_count=0,
        reference_finding_count=0,
        migrated_finding_count=0,
        parity_result=parity_result,
        elapsed_seconds=0.1,
    )


def test_prepare_artifacts_dir_recreates_latest_tree(tmp_path: Path) -> None:
    stale_file = tmp_path / "artifacts" / "latest" / "stale.txt"
    stale_file.parent.mkdir(parents=True)
    stale_file.write_text("stale", encoding="utf-8")

    artifacts_dir, site_dir = prepare_artifacts_dir(tmp_path)

    assert artifacts_dir == tmp_path / "artifacts" / "latest"
    assert site_dir == artifacts_dir / "site"
    assert site_dir.is_dir()
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

    def fake_source_snapshot_id_for(_: Path) -> str:
        return "snapshot-123"

    def fake_count_source_rows(_: Path) -> int:
        return 42

    def fake_configured_check_profile_name() -> str:
        return "ignored"

    def fake_get_default_check_catalog() -> CheckCatalog:
        return default_check_catalog

    def fake_load_check_profile(
        config_path: Path,
        profile_name: str | None = None,
        *,
        catalog: CheckCatalog | None = None,
    ) -> ActiveCheckProfile:
        assert config_path == tmp_path / "config" / "check-profiles.toml"
        assert profile_name == "ignored"
        assert catalog is default_check_catalog
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
        "configured_check_profile_name",
        fake_configured_check_profile_name,
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

    prepared = prepare_run(
        tmp_path,
        tmp_path / "data" / "products.duckdb",
        logger=logging.getLogger("test-pipeline"),
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
    assert prepared.requires_enriched_snapshots is False
    assert prepared.requires_reference_findings is True
    assert prepared.requires_reference_results is True


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
        reference_result_cache_key="",
        reference_result_cache_path=Path(),
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
    parity_result_factory: ParityResultFactory,
) -> None:
    scheduler = BatchScheduler(
        batch_iterator=iter(
            [
                ScheduledBatch(batch_index=1, rows=[{"code": "1"}]),
                ScheduledBatch(batch_index=2, rows=[{"code": "2"}]),
            ]
        ),
        executor=_ImmediateExecutor(),
        process_batch=lambda batch: _batch_execution_result(
            batch,
            parity_result_factory(),
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
    parity_result_factory: ParityResultFactory,
    tmp_path: Path,
) -> None:
    def fake_iter_source_batches(
        _: Path, *, batch_size: int
    ) -> list[list[dict[str, str]]]:
        assert batch_size == 2
        return [
            [{"code": "1"}],
            [{"code": "2"}, {"code": "3"}],
        ]

    def fake_execute_batch(
        batch: ScheduledBatch,
        _: BatchExecutionContext,
    ) -> BatchExecutionResult:
        return _batch_execution_result(batch, parity_result_factory())

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

    run_batches(
        plan=BatchRunPlan(
            db_path=tmp_path / "products.duckdb",
            batch_size=2,
            legacy_backend_workers=2,
        ),
        execution=_dummy_execution_context(),
        execution_progress=progress,
        accumulator=accumulator,
    )

    assert progress.completed_batches == [(1, 1), (2, 3)]
    assert progress.heartbeats == []
    assert len(accumulator.parity_results) == 2


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
    parity_result_factory: ParityResultFactory,
    reference_result_factory: Callable[..., ReferenceResult],
) -> None:
    batch = ScheduledBatch(
        batch_index=1,
        rows=[{"code": "123", "product_name": "Raw name"}],
    )
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
    expected_parity = parity_result_factory()
    result = _execute_batch_with_stubbed_runtime(
        batch=batch,
        execution=BatchExecutionContext(
            batch_input_resolver=(
                ReferenceBatchInputResolver(
                    reference_result_loader=reference_result_loader,
                    reference_observer=reference_observer,
                    include_enriched_snapshots=True,
                )
                if reference_result_loader is not None
                else NoOpBatchInputResolver()
            ),
            evaluators={},
            active_checks=(),
            check_context_builder=check_context_builder_for(input_surface),
            run_id="run",
            source_snapshot_id="source-snapshot",
        ),
        expected_parity=expected_parity,
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
    assert result.parity_result == expected_parity


def test_execute_batch_loads_reference_results_for_legacy_parity_without_enriched_contexts(
    monkeypatch: pytest.MonkeyPatch,
    parity_result_factory: ParityResultFactory,
    reference_result_factory: Callable[..., ReferenceResult],
) -> None:
    batch = ScheduledBatch(
        batch_index=1,
        rows=[{"code": "123", "product_name": "Raw name"}],
    )
    reference_result_loader = _RecordingReferenceResultLoader(
        [reference_result_factory(code="123")]
    )
    reference_observer = _RecordingReferenceObserver(requires_reference_results=True)
    expected_parity = parity_result_factory()
    result = _execute_batch_with_stubbed_runtime(
        batch=batch,
        execution=BatchExecutionContext(
            batch_input_resolver=ReferenceBatchInputResolver(
                reference_result_loader=reference_result_loader,
                reference_observer=reference_observer,
                include_enriched_snapshots=False,
            ),
            evaluators={},
            active_checks=(),
            check_context_builder=check_context_builder_for("raw_products"),
            run_id="run",
            source_snapshot_id="source-snapshot",
        ),
        expected_parity=expected_parity,
        monkeypatch=monkeypatch,
    )

    assert [
        [product.code for product in call] for call in reference_result_loader.calls
    ] == [["123"]]
    assert [result.code for result in reference_observer.calls[0]] == ["123"]
    assert result.parity_result == expected_parity


def test_execute_batch_resolves_enrichment_and_reference_from_one_batch_input_loader(
    monkeypatch: pytest.MonkeyPatch,
    parity_result_factory: ParityResultFactory,
    reference_result_factory: Callable[..., ReferenceResult],
    observed_finding_factory: Callable[..., ObservedFinding],
) -> None:
    batch = ScheduledBatch(
        batch_index=1,
        rows=[{"code": "123", "product_name": "Raw name"}],
    )
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
    result = _execute_batch_with_stubbed_runtime(
        batch=batch,
        execution=BatchExecutionContext(
            batch_input_resolver=ReferenceBatchInputResolver(
                reference_result_loader=reference_result_loader,
                reference_observer=reference_observer,
                include_enriched_snapshots=True,
            ),
            evaluators={},
            active_checks=(),
            check_context_builder=check_context_builder_for("enriched_products"),
            run_id="run",
            source_snapshot_id="source-snapshot",
        ),
        expected_parity=parity_result_factory(),
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
