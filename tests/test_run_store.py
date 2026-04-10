from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import duckdb
import pytest
from migration.planning import (
    ActiveMigrationPlan,
    MigrationAssessment,
    MigrationFamily,
)
from migration.reference.observers import NoReferenceObserver
from migration.report.renderer import render_report_from_store
from migration.run.context_builders import check_context_builder_for
from migration.run.models import (
    BatchExecutionResult,
    BatchStageTimings,
    PreparedRun,
    RunPreparationTimings,
    RunSpec,
)
from migration.run.profiles import ActiveCheckProfile
from migration.source.datasets import ActiveDatasetProfile, SourceSelection
from migration.source.models import SkippedSourceRow, SourceInputSummary
from migration.storage import (
    load_recorded_run_benchmark_summary,
    load_recorded_run_snapshot,
)
from migration.storage.run_store import PARITY_STORE_SCHEMA_VERSION, DuckDBRunRecorder

from off_data_quality.contracts.run import RunResult


def test_duckdb_run_recorder_persists_batches_mismatches_and_final_summary(
    tmp_path: Path,
    run_result_factory: Callable[[], RunResult],
) -> None:
    run_result = run_result_factory()
    parity_store_path = _record_completed_run(tmp_path, run_result=run_result)

    connection = duckdb.connect(str(parity_store_path), read_only=True)
    try:
        run_row = connection.execute(
            """
            select
                status,
                active_check_profile_name,
                run_artifact_json,
                prepare_run_seconds,
                source_snapshot_id_seconds,
                dataset_profile_load_seconds,
                source_row_count_seconds
            from runs
            where run_id = ?
            """,
            [run_result.run_id],
        ).fetchone()
        assert run_row is not None
        assert run_row[:2] == ("completed", "focused")
        assert run_row[3:] == (0.11, 0.12, 0.13, 0.14)

        run_artifact = json.loads(str(run_row[2]))
        assert run_artifact["run_id"] == run_result.run_id
        assert run_artifact["source_snapshot_id"] == run_result.source_snapshot_id
        assert run_artifact["source_input"] == {
            "processed_product_count": run_result.product_count,
            "skipped_row_count": 2,
            "skipped_row_examples": [
                {
                    "location": "jsonl line 4",
                    "reason": "missing or blank code",
                }
            ],
        }

        dataset_row = connection.execute(
            """
            select
                profile_name,
                selection_kind,
                selection_fingerprint
            from run_dataset_profiles
            where run_id = ?
            """,
            [run_result.run_id],
        ).fetchone()
        assert dataset_row is not None
        assert dataset_row[0] == "validation"
        assert dataset_row[1] == "stable_sample"
        assert str(dataset_row[2]).startswith("sha256:")

        migration_row = connection.execute(
            """
            select
                check_id,
                target_impl,
                size,
                risk,
                is_assessed
            from run_active_migration_families
            where run_id = ?
            order by check_id
            """,
            [run_result.run_id],
        ).fetchone()
        assert migration_row == (
            "en:quantity-not-recognized",
            "dsl",
            "S",
            "low",
            True,
        )

        mismatch_rows = connection.execute(
            """
            select
                check_id,
                mismatch_kind,
                observation_side,
                product_id,
                observed_code,
                severity
            from run_mismatches
            where run_id = ?
            order by check_id, observed_code
            """,
            [run_result.run_id],
        ).fetchall()
        assert mismatch_rows == [
            (
                "en:quantity-not-recognized",
                "missing",
                "reference",
                "123",
                "en:quantity-not-recognized",
                "warning",
            )
        ]

        batch_row = connection.execute(
            """
            select
                source_read_seconds,
                reference_load_seconds,
                reference_check_context_materialization_seconds,
                reference_finding_materialization_seconds,
                migrated_findings_seconds,
                parity_compare_seconds,
                record_batch_seconds
            from run_batches
            where run_id = ? and batch_index = 1
            """,
            [run_result.run_id],
        ).fetchone()
        assert batch_row is not None
        assert batch_row[:6] == (0.01, 0.02, 0.03, 0.04, 0.05, 0.06)
        assert float(batch_row[6]) >= 0.0

        summary_rows = connection.execute(
            """
            select
                check_id,
                missing_count,
                extra_count
            from run_check_summaries
            where run_id = ?
            order by check_id
            """,
            [run_result.run_id],
        ).fetchall()
        assert summary_rows == [
            ("en:product-name-to-be-completed", 0, 0),
            ("en:quantity-not-recognized", 1, 0),
        ]
    finally:
        connection.close()


def test_duckdb_run_recorder_marks_failed_runs_when_execution_aborts(
    tmp_path: Path,
    run_result_factory: Callable[[], RunResult],
) -> None:
    run_result = run_result_factory()
    parity_store_path = tmp_path / "parity-store.duckdb"
    prepared_run = _prepared_run_for_result(run_result, tmp_path)
    run_spec = _run_spec_for_store(tmp_path, parity_store_path=parity_store_path)

    with pytest.raises(RuntimeError, match="boom"):
        with DuckDBRunRecorder(
            path=parity_store_path,
            run_spec=run_spec,
            prepared_run=prepared_run,
        ):
            raise RuntimeError("boom")

    connection = duckdb.connect(str(parity_store_path), read_only=True)
    try:
        status_row = connection.execute(
            "select status, failure_type, failure_message from runs where run_id = ?",
            [run_result.run_id],
        ).fetchone()
        assert status_row == ("failed", "RuntimeError", "boom")
    finally:
        connection.close()


def test_load_recorded_run_snapshot_reads_store_backed_metadata(
    tmp_path: Path,
    run_result_factory: Callable[[], RunResult],
) -> None:
    run_result = run_result_factory()
    parity_store_path = _record_completed_run(tmp_path, run_result=run_result)

    snapshot = load_recorded_run_snapshot(parity_store_path, run_id=run_result.run_id)

    assert snapshot.run_result == run_result
    assert snapshot.dataset_profile is not None
    assert snapshot.dataset_profile.name == "validation"
    assert snapshot.dataset_profile.selection_kind == "stable_sample"
    assert snapshot.active_migration_family_count == 1
    assert snapshot.assessed_migration_family_count == 1
    assert snapshot.unmatched_migration_check_count == 1
    assert (
        snapshot.migration_families_by_check_id[
            "en:quantity-not-recognized"
        ].target_impl
        == "dsl"
    )


def test_load_recorded_run_benchmark_summary_reads_stage_timings(
    tmp_path: Path,
    run_result_factory: Callable[[], RunResult],
) -> None:
    run_result = run_result_factory()
    parity_store_path = _record_completed_run(tmp_path, run_result=run_result)

    summary = load_recorded_run_benchmark_summary(
        parity_store_path,
        run_id=run_result.run_id,
    )

    assert summary.run_id == run_result.run_id
    assert summary.status == "completed"
    assert summary.product_count == run_result.product_count
    assert summary.prepare_run_seconds == 0.11
    assert summary.source_snapshot_id_seconds == 0.12
    assert summary.dataset_profile_load_seconds == 0.13
    assert summary.source_row_count_seconds == 0.14
    assert summary.batch_count == 1
    assert summary.batch_elapsed_seconds == 0.25
    assert summary.source_read_seconds == 0.01
    assert summary.reference_load_seconds == 0.02
    assert summary.reference_check_context_materialization_seconds == 0.03
    assert summary.reference_finding_materialization_seconds == 0.04
    assert summary.migrated_findings_seconds == 0.05
    assert summary.parity_compare_seconds == 0.06
    assert summary.record_batch_seconds >= 0.0


def test_render_report_from_store_uses_store_backed_snapshot(
    tmp_path: Path,
    run_result_factory: Callable[[], RunResult],
    legacy_source_root_factory: Callable[[Path], Path],
) -> None:
    run_result = run_result_factory()
    parity_store_path = _record_completed_run(tmp_path, run_result=run_result)
    output_dir = tmp_path / "site"

    render_report_from_store(
        store_path=parity_store_path,
        run_id=run_result.run_id,
        output_dir=output_dir,
        legacy_source_root=legacy_source_root_factory(tmp_path),
    )

    html = (output_dir / "index.html").read_text(encoding="utf-8")
    run_artifact = json.loads((output_dir / "run.json").read_text(encoding="utf-8"))

    assert "Policy Rules" not in html
    assert "Expected differences:" not in html
    assert "Unexpected differences:" not in html
    assert run_artifact["run_id"] == run_result.run_id
    assert run_artifact["source_snapshot_id"] == run_result.source_snapshot_id


def _seed_parity_store_meta(path: Path, *, schema_version: int) -> None:
    """Create one minimal parity-store metadata table at the requested version."""
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """
            create table parity_store_meta (
                schema_version integer not null,
                created_at_utc varchar not null
            )
            """
        )
        connection.execute(
            "insert into parity_store_meta values (?, ?)",
            [schema_version, "2026-01-01T00:00:00Z"],
        )
    finally:
        connection.close()


def _open_run_recorder_once(
    *,
    tmp_path: Path,
    parity_store_path: Path,
    run_result: RunResult,
) -> None:
    """Open one recorder instance so store bootstrap logic runs once."""
    with DuckDBRunRecorder(
        path=parity_store_path,
        run_spec=_run_spec_for_store(tmp_path, parity_store_path=parity_store_path),
        prepared_run=_prepared_run_for_result(run_result, tmp_path),
    ):
        pass


def test_duckdb_run_recorder_recreates_legacy_store_schema(
    tmp_path: Path,
    run_result_factory: Callable[[], RunResult],
) -> None:
    parity_store_path = tmp_path / "parity-store.duckdb"
    _seed_parity_store_meta(parity_store_path, schema_version=2)

    run_result = run_result_factory()
    _open_run_recorder_once(
        tmp_path=tmp_path,
        parity_store_path=parity_store_path,
        run_result=run_result,
    )

    connection = duckdb.connect(str(parity_store_path))
    try:
        rows = connection.execute(
            "select schema_version from parity_store_meta"
        ).fetchall()
    finally:
        connection.close()

    assert rows == [(PARITY_STORE_SCHEMA_VERSION,)]


def test_duckdb_run_recorder_recreates_same_version_store_with_missing_columns(
    tmp_path: Path,
    run_result_factory: Callable[[], RunResult],
) -> None:
    parity_store_path = tmp_path / "parity-store.duckdb"
    _seed_parity_store_meta(
        parity_store_path,
        schema_version=PARITY_STORE_SCHEMA_VERSION,
    )

    connection = duckdb.connect(str(parity_store_path))
    try:
        connection.execute(
            """
            create table runs (
                run_id varchar primary key,
                requires_reference_results boolean not null,
                requires_reference_findings boolean not null
            )
            """
        )
        connection.execute(
            """
            create table run_batches (
                run_id varchar not null,
                batch_index integer not null,
                reference_load_seconds double not null,
                reference_finding_materialization_seconds double not null
            )
            """
        )
    finally:
        connection.close()

    run_result = run_result_factory()
    _open_run_recorder_once(
        tmp_path=tmp_path,
        parity_store_path=parity_store_path,
        run_result=run_result,
    )

    connection = duckdb.connect(str(parity_store_path))
    try:
        runs_columns = {
            str(row[1])
            for row in connection.execute("pragma table_info('runs')").fetchall()
        }
        batch_columns = {
            str(row[1])
            for row in connection.execute("pragma table_info('run_batches')").fetchall()
        }
    finally:
        connection.close()

    assert "requires_reference_check_contexts" in runs_columns
    assert "reference_check_context_materialization_seconds" in batch_columns


def _prepared_run_for_result(run_result: RunResult, tmp_path: Path) -> PreparedRun:
    """Return a minimal prepared run compatible with one synthetic run result."""
    checks = tuple(check.definition for check in run_result.checks)
    return PreparedRun(
        source_snapshot_id=run_result.source_snapshot_id,
        run_id=run_result.run_id,
        product_count=run_result.product_count,
        active_check_profile=ActiveCheckProfile(
            name="focused",
            description="Focused test profile",
            check_context_provider="source_products",
            parity_baselines=("legacy",),
            jurisdictions=None,
            check_ids=tuple(check.id for check in checks),
            checks=checks,
        ),
        check_context_builder=check_context_builder_for("source_products"),
        reference_observer=NoReferenceObserver(),
        evaluators={},
        reference_result_cache_key="cache-key",
        reference_result_cache_path=tmp_path / "reference-cache.duckdb",
        python_count=sum(
            1 for check in checks if check.definition_language == "python"
        ),
        dsl_count=sum(1 for check in checks if check.definition_language == "dsl"),
        legacy_parity_count=sum(
            1 for check in checks if check.parity_baseline == "legacy"
        ),
        runtime_only_count=sum(
            1 for check in checks if check.parity_baseline == "none"
        ),
        source_input_summary=SourceInputSummary(
            processed_product_count=run_result.product_count,
            skipped_row_count=2,
            skipped_row_examples=(
                SkippedSourceRow(
                    location="jsonl line 4",
                    reason="missing or blank code",
                ),
            ),
        ),
        preparation_timings=RunPreparationTimings(
            prepare_run_seconds=0.11,
            source_snapshot_id_seconds=0.12,
            dataset_profile_load_seconds=0.13,
            source_row_count_seconds=0.14,
        ),
        active_dataset_profile=ActiveDatasetProfile(
            name="validation",
            description="Deterministic validation sample",
            selection=SourceSelection(
                kind="stable_sample",
                sample_size=1000,
                seed=42,
            ),
        ),
        active_migration_plan=ActiveMigrationPlan(
            families=(
                MigrationFamily(
                    check_id="en:quantity-not-recognized",
                    template_key="en:quantity-not-recognized",
                    code_templates=("en:quantity-not-recognized",),
                    placeholder_names=(),
                    placeholder_count=0,
                    has_loop=False,
                    has_branching=False,
                    has_arithmetic=False,
                    helper_calls=(),
                    source_files_count=1,
                    source_subroutines_count=1,
                    unsupported_data_quality_emission_count_total=0,
                    line_span_max=12,
                    statement_count_max=4,
                    assessment=MigrationAssessment(
                        target_impl="dsl",
                        size="S",
                        risk="low",
                        estimated_hours="1",
                        rationale="Straightforward template migration.",
                    ),
                ),
            ),
            missing_check_ids=("en:product-name-to-be-completed",),
        ),
    )


def _run_spec_for_store(
    tmp_path: Path,
    *,
    parity_store_path: Path,
) -> RunSpec:
    """Return a stable run spec for store-level tests."""
    return RunSpec(
        project_root=tmp_path,
        db_path=tmp_path / "source.duckdb",
        batch_size=100,
        mismatch_examples_limit=5,
        batch_workers=2,
        legacy_backend_workers=1,
        reference_result_cache_dir=tmp_path / "reference-cache",
        reference_result_cache_salt="salt",
        parity_store_path=parity_store_path,
    )


def _record_completed_run(
    tmp_path: Path,
    *,
    run_result: RunResult,
) -> Path:
    """Record one completed run in the store and return the store path."""
    parity_store_path = tmp_path / "parity-store.duckdb"
    run_spec = _run_spec_for_store(tmp_path, parity_store_path=parity_store_path)
    with DuckDBRunRecorder(
        path=parity_store_path,
        run_spec=run_spec,
        prepared_run=_prepared_run_for_result(run_result, tmp_path),
    ) as recorder:
        recorder.record_batch(
            BatchExecutionResult(
                batch_index=1,
                row_count=run_result.product_count,
                cache_hit_count=0,
                backend_run_count=1,
                reference_finding_count=run_result.reference_total,
                migrated_finding_count=run_result.compared_migrated_total,
                run_result=run_result,
                elapsed_seconds=0.25,
                stage_timings=BatchStageTimings(
                    source_read_seconds=0.01,
                    reference_load_seconds=0.02,
                    reference_check_context_materialization_seconds=0.03,
                    reference_finding_materialization_seconds=0.04,
                    migrated_findings_seconds=0.05,
                    parity_compare_seconds=0.06,
                ),
            )
        )
        recorder.record_final_result(run_result)
    return parity_store_path
