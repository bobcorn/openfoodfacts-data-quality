from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import duckdb
import pytest
from app.migration.catalog import (
    ActiveMigrationPlan,
    MigrationAssessment,
    MigrationFamily,
)
from app.parity.policy import (
    ExpectedDifferenceRule,
    ExpectedDifferencesRegistry,
)
from app.reference.observers import NoReferenceObserver
from app.report.renderer import render_report_from_store
from app.run.context_builders import check_context_builder_for
from app.run.models import BatchExecutionResult, PreparedRun, RunSpec
from app.run.profiles import ActiveCheckProfile
from app.source.datasets import ActiveDatasetProfile, SourceSelection
from app.storage import load_recorded_run_snapshot
from app.storage.run_store import DuckDBRunRecorder

from openfoodfacts_data_quality.contracts.run import RunResult


def test_duckdb_run_recorder_persists_batches_mismatches_and_final_summary(
    tmp_path: Path,
    run_result_factory: Callable[[], RunResult],
) -> None:
    run_result = run_result_factory()
    registry_path = tmp_path / "expected-differences.toml"
    registry_path.write_text("# stub", encoding="utf-8")
    registry = ExpectedDifferencesRegistry(
        rules=(
            ExpectedDifferenceRule(
                id="quantity-known-gap",
                justification="Known mismatch under review.",
                check_ids=("en:quantity-not-recognized",),
                mismatch_kinds=("missing",),
            ),
        ),
        source_path=registry_path,
    )
    parity_store_path = _record_completed_run(
        tmp_path,
        run_result=run_result,
        expected_differences=registry,
    )

    connection = duckdb.connect(str(parity_store_path), read_only=True)
    try:
        run_row = connection.execute(
            """
            select
                status,
                active_check_profile_name,
                expected_differences_rule_count,
                run_artifact_json
            from runs
            where run_id = ?
            """,
            [run_result.run_id],
        ).fetchone()
        assert run_row is not None
        assert run_row[:3] == ("completed", "focused", 1)

        run_artifact = json.loads(str(run_row[3]))
        assert run_artifact["run_id"] == run_result.run_id
        assert run_artifact["source_snapshot_id"] == run_result.source_snapshot_id

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
                severity,
                expected_rule_id
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
                "quantity-known-gap",
            )
        ]

        summary_rows = connection.execute(
            """
            select
                check_id,
                missing_count,
                extra_count,
                expected_missing_count,
                unexpected_missing_count,
                expected_extra_count,
                unexpected_extra_count
            from run_check_summaries
            where run_id = ?
            order by check_id
            """,
            [run_result.run_id],
        ).fetchall()
        assert summary_rows == [
            ("en:product-name-to-be-completed", 0, 0, 0, 0, 0, 0),
            ("en:quantity-not-recognized", 1, 0, 1, 0, 0, 0),
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
            expected_differences=ExpectedDifferencesRegistry(),
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


def test_load_recorded_run_snapshot_reads_store_backed_governance(
    tmp_path: Path,
    run_result_factory: Callable[[], RunResult],
) -> None:
    run_result = run_result_factory()
    registry = _expected_gap_registry()
    parity_store_path = _record_completed_run(
        tmp_path,
        run_result=run_result,
        expected_differences=registry,
    )

    snapshot = load_recorded_run_snapshot(parity_store_path, run_id=run_result.run_id)

    assert snapshot.run_result == run_result
    assert snapshot.dataset_profile is not None
    assert snapshot.dataset_profile.name == "validation"
    assert snapshot.dataset_profile.selection_kind == "stable_sample"
    assert snapshot.expected_differences_rule_count == 1
    assert snapshot.expected_mismatch_total == 1
    assert snapshot.unexpected_mismatch_total == 0
    assert snapshot.active_migration_family_count == 1
    assert snapshot.assessed_migration_family_count == 1
    assert snapshot.unmatched_migration_check_count == 1
    assert (
        snapshot.migration_families_by_check_id[
            "en:quantity-not-recognized"
        ].target_impl
        == "dsl"
    )
    assert (
        snapshot.check_governance_by_id[
            "en:quantity-not-recognized"
        ].expected_missing_count
        == 1
    )


def test_render_report_from_store_uses_store_backed_snapshot(
    tmp_path: Path,
    run_result_factory: Callable[[], RunResult],
    legacy_source_root_factory: Callable[[Path], Path],
) -> None:
    run_result = run_result_factory()
    registry = _expected_gap_registry()
    parity_store_path = _record_completed_run(
        tmp_path,
        run_result=run_result,
        expected_differences=registry,
    )
    output_dir = tmp_path / "site"

    render_report_from_store(
        store_path=parity_store_path,
        run_id=run_result.run_id,
        output_dir=output_dir,
        legacy_source_root=legacy_source_root_factory(tmp_path),
    )

    html = (output_dir / "index.html").read_text(encoding="utf-8")
    run_artifact = json.loads((output_dir / "run.json").read_text(encoding="utf-8"))

    assert "Policy Rules" in html
    assert "Expected differences: 1 missing, 0 extra." in html
    assert run_artifact["run_id"] == run_result.run_id
    assert run_artifact["source_snapshot_id"] == run_result.source_snapshot_id


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
            check_input_surface="raw_products",
            parity_baselines=("legacy",),
            jurisdictions=None,
            check_ids=tuple(check.id for check in checks),
            checks=checks,
        ),
        check_context_builder=check_context_builder_for("raw_products"),
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
    expected_differences_path: Path | None = None,
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
        expected_differences_path=expected_differences_path,
    )


def _record_completed_run(
    tmp_path: Path,
    *,
    run_result: RunResult,
    expected_differences: ExpectedDifferencesRegistry,
) -> Path:
    """Record one completed run in the store and return the store path."""
    parity_store_path = tmp_path / "parity-store.duckdb"
    run_spec = _run_spec_for_store(
        tmp_path,
        parity_store_path=parity_store_path,
        expected_differences_path=expected_differences.source_path,
    )
    with DuckDBRunRecorder(
        path=parity_store_path,
        run_spec=run_spec,
        prepared_run=_prepared_run_for_result(run_result, tmp_path),
        expected_differences=expected_differences,
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
            )
        )
        recorder.record_final_result(run_result)
    return parity_store_path


def _expected_gap_registry() -> ExpectedDifferencesRegistry:
    """Return one registry that marks the quantity gap as expected."""
    return ExpectedDifferencesRegistry(
        rules=(
            ExpectedDifferenceRule(
                id="quantity-known-gap",
                justification="Known mismatch under review.",
                check_ids=("en:quantity-not-recognized",),
                mismatch_kinds=("missing",),
            ),
        )
    )
