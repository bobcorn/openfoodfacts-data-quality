from __future__ import annotations

import json
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING

import duckdb

from migration.run.serialization import build_run_artifact

if TYPE_CHECKING:
    from types import TracebackType

    from migration.run.models import BatchExecutionResult, PreparedRun, RunSpec
    from off_data_quality.contracts.observations import ObservedFinding
    from off_data_quality.contracts.run import RunCheckResult, RunResult

PARITY_STORE_SCHEMA_VERSION = 2


class NoopRunRecorder(AbstractContextManager["NoopRunRecorder"]):
    """Recorder used when the current migration run does not persist a run store."""

    def __enter__(self) -> NoopRunRecorder:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        return None

    def record_batch(self, batch_result: BatchExecutionResult) -> None:
        """Ignore one merged batch result."""
        del batch_result

    def record_final_result(self, run_result: RunResult) -> None:
        """Ignore one finalized run result."""
        del run_result


class DuckDBRunRecorder(AbstractContextManager["DuckDBRunRecorder"]):
    """Persist one migration run into a project owned DuckDB parity store."""

    def __init__(
        self,
        *,
        path: Path,
        run_spec: RunSpec,
        prepared_run: PreparedRun,
    ) -> None:
        self.path = path
        self.run_spec = run_spec
        self.prepared_run = prepared_run
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._finalized = False

    def __enter__(self) -> DuckDBRunRecorder:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        connection = self._connection
        if connection is None:
            return None
        try:
            if not self._finalized:
                if exc is None:
                    self._mark_run_terminated(status="incomplete")
                else:
                    self._mark_run_terminated(
                        status="failed",
                        failure_type=exc_type.__name__
                        if exc_type is not None
                        else None,
                        failure_message=str(exc),
                    )
        finally:
            connection.close()
            self._connection = None
        return None

    def start(self) -> None:
        """Create the store schema and register the current run when needed."""
        if self._connection is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = duckdb.connect(str(self.path))
        self._ensure_store_schema()
        self._register_run()
        self._snapshot_dataset_profile()

    def record_batch(self, batch_result: BatchExecutionResult) -> None:
        """Persist one merged batch and its concrete mismatches."""
        connection = self._require_connection()
        started = perf_counter()
        mismatch_rows = self._build_mismatch_rows(batch_result)
        connection.execute("begin")
        try:
            connection.execute(
                """
                insert into run_batches (
                    run_id,
                    batch_index,
                    row_count,
                    cache_hit_count,
                    backend_run_count,
                    reference_finding_count,
                    migrated_finding_count,
                    elapsed_seconds,
                    source_read_seconds,
                    reference_load_seconds,
                    reference_check_context_materialization_seconds,
                    reference_finding_materialization_seconds,
                    migrated_findings_seconds,
                    parity_compare_seconds,
                    record_batch_seconds
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    self.prepared_run.run_id,
                    batch_result.batch_index,
                    batch_result.row_count,
                    batch_result.cache_hit_count,
                    batch_result.backend_run_count,
                    batch_result.reference_finding_count,
                    batch_result.migrated_finding_count,
                    batch_result.elapsed_seconds,
                    batch_result.stage_timings.source_read_seconds,
                    batch_result.stage_timings.reference_load_seconds,
                    batch_result.stage_timings.reference_check_context_materialization_seconds,
                    batch_result.stage_timings.reference_finding_materialization_seconds,
                    batch_result.stage_timings.migrated_findings_seconds,
                    batch_result.stage_timings.parity_compare_seconds,
                    0.0,
                ],
            )
            if mismatch_rows:
                connection.executemany(
                    """
                    insert into run_mismatches (
                        run_id,
                        batch_index,
                        check_id,
                        mismatch_kind,
                        observation_side,
                        product_id,
                        observed_code,
                        severity
                    )
                    values (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    mismatch_rows,
                )
            connection.execute("commit")
        except Exception:
            connection.execute("rollback")
            raise
        connection.execute(
            """
            update run_batches
            set record_batch_seconds = ?
            where run_id = ? and batch_index = ?
            """,
            [
                perf_counter() - started,
                self.prepared_run.run_id,
                batch_result.batch_index,
            ],
        )

    def record_final_result(self, run_result: RunResult) -> None:
        """Persist the finalized summary for each check and mark the run complete."""
        connection = self._require_connection()
        check_summary_rows = [
            self._check_summary_row(check_result) for check_result in run_result.checks
        ]
        run_artifact_json = json.dumps(
            build_run_artifact(
                run_result,
                source_input_summary=self.prepared_run.source_input_summary,
            ),
            ensure_ascii=False,
        )
        connection.execute("begin")
        try:
            if check_summary_rows:
                connection.executemany(
                    """
                    insert into run_check_summaries (
                        run_id,
                        check_id,
                        definition_language,
                        parity_baseline,
                        comparison_status,
                        passed,
                        reference_count,
                        migrated_count,
                        matched_count,
                        missing_count,
                        extra_count,
                        legacy_identity_code_template
                    )
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    check_summary_rows,
                )

            connection.execute(
                """
                update runs
                set
                    status = ?,
                    completed_at_utc = ?,
                    compared_check_count = ?,
                    runtime_only_check_count = ?,
                    reference_total = ?,
                    compared_migrated_total = ?,
                    matched_total = ?,
                    runtime_only_migrated_total = ?,
                    run_artifact_json = ?,
                    failure_type = null,
                    failure_message = null
                where run_id = ?
                """,
                [
                    "completed",
                    _utc_now(),
                    run_result.compared_check_count,
                    run_result.runtime_only_check_count,
                    run_result.reference_total,
                    run_result.compared_migrated_total,
                    run_result.matched_total,
                    run_result.runtime_only_migrated_total,
                    run_artifact_json,
                    self.prepared_run.run_id,
                ],
            )
            connection.execute("commit")
        except Exception:
            connection.execute("rollback")
            raise
        self._finalized = True

    def _ensure_store_schema(self) -> None:
        """Create or validate the run store schema."""
        connection = self._require_connection()
        self._create_store_metadata_schema()
        rows = connection.execute(
            "select schema_version from parity_store_meta"
        ).fetchall()
        if not rows:
            self._recreate_store_schema()
            return
        if len(rows) != 1:
            self._recreate_store_schema()
            return
        schema_version = int(rows[0][0])
        if schema_version != PARITY_STORE_SCHEMA_VERSION:
            self._recreate_store_schema()
            return
        if not self._matches_current_store_schema():
            self._recreate_store_schema()
            return
        self._create_current_store_schema()

    def _create_fresh_store_schema(self) -> None:
        """Create the current schema and metadata row in an empty store."""
        connection = self._require_connection()
        self._create_store_metadata_schema()
        self._create_current_store_schema()
        connection.execute(
            "insert into parity_store_meta values (?, ?)",
            [PARITY_STORE_SCHEMA_VERSION, _utc_now()],
        )

    def _create_store_metadata_schema(self) -> None:
        """Ensure the local store metadata table exists."""
        connection = self._require_connection()
        connection.execute(
            """
            create table if not exists parity_store_meta (
                schema_version integer not null,
                created_at_utc varchar not null
            )
            """
        )

    def _recreate_store_schema(self) -> None:
        """Replace an obsolete local store with the current unreleased schema."""
        connection = self._require_connection()
        connection.close()
        self._connection = None
        self.path.unlink(missing_ok=True)
        self._connection = duckdb.connect(str(self.path))
        self._create_fresh_store_schema()

    def _matches_current_store_schema(self) -> bool:
        """Return whether the persisted store exposes the required current columns."""
        return self._table_has_columns(
            "runs",
            (
                "requires_reference_results",
                "requires_reference_check_contexts",
                "requires_reference_findings",
            ),
        ) and self._table_has_columns(
            "run_batches",
            (
                "reference_load_seconds",
                "reference_check_context_materialization_seconds",
                "reference_finding_materialization_seconds",
            ),
        )

    def _table_has_columns(
        self,
        table_name: str,
        required_columns: tuple[str, ...],
    ) -> bool:
        """Return whether one persisted table exposes every required column."""
        connection = self._require_connection()
        try:
            rows = connection.execute(f"pragma table_info('{table_name}')").fetchall()
        except duckdb.Error:
            return False
        existing_columns = {str(row[1]) for row in rows}
        return all(column in existing_columns for column in required_columns)

    def _create_current_store_schema(self) -> None:
        """Ensure that every current store table and index exists."""
        connection = self._require_connection()
        connection.execute(
            """
            create table if not exists runs (
                run_id varchar primary key,
                status varchar not null,
                recorded_at_utc varchar not null,
                completed_at_utc varchar,
                source_snapshot_id varchar not null,
                product_count integer not null,
                prepare_run_seconds double not null,
                source_snapshot_id_seconds double not null,
                dataset_profile_load_seconds double not null,
                source_row_count_seconds double not null,
                source_db_path varchar not null,
                requested_check_profile_name varchar,
                active_check_profile_name varchar not null,
                check_context_provider varchar not null,
                batch_size integer not null,
                batch_workers integer not null,
                legacy_backend_workers integer not null,
                mismatch_examples_limit integer not null,
                reference_result_cache_key varchar,
                reference_result_cache_path varchar,
                requires_reference_results boolean not null,
                requires_reference_check_contexts boolean not null,
                requires_reference_findings boolean not null,
                python_check_count integer not null,
                dsl_check_count integer not null,
                legacy_parity_check_count integer not null,
                runtime_only_check_count integer not null,
                requested_dataset_profile_name varchar,
                active_dataset_profile_name varchar,
                active_dataset_selection_kind varchar,
                active_dataset_selection_fingerprint varchar,
                compared_check_count integer,
                reference_total integer,
                compared_migrated_total integer,
                matched_total integer,
                runtime_only_migrated_total integer,
                run_artifact_json text,
                failure_type varchar,
                failure_message text
            )
            """
        )
        connection.execute(
            """
            create table if not exists run_batches (
                run_id varchar not null,
                batch_index integer not null,
                row_count integer not null,
                cache_hit_count integer not null,
                backend_run_count integer not null,
                reference_finding_count integer not null,
                migrated_finding_count integer not null,
                elapsed_seconds double not null,
                source_read_seconds double not null,
                reference_load_seconds double not null,
                reference_check_context_materialization_seconds double not null,
                reference_finding_materialization_seconds double not null,
                migrated_findings_seconds double not null,
                parity_compare_seconds double not null,
                record_batch_seconds double not null,
                primary key (run_id, batch_index)
            )
            """
        )
        connection.execute(
            """
            create table if not exists run_mismatches (
                run_id varchar not null,
                batch_index integer not null,
                check_id varchar not null,
                mismatch_kind varchar not null,
                observation_side varchar not null,
                product_id varchar not null,
                observed_code varchar not null,
                severity varchar not null
            )
            """
        )
        connection.execute(
            """
            create table if not exists run_check_summaries (
                run_id varchar not null,
                check_id varchar not null,
                definition_language varchar not null,
                parity_baseline varchar not null,
                comparison_status varchar not null,
                passed boolean,
                reference_count integer not null,
                migrated_count integer not null,
                matched_count integer not null,
                missing_count integer not null,
                extra_count integer not null,
                legacy_identity_code_template varchar,
                primary key (run_id, check_id)
            )
            """
        )
        connection.execute(
            """
            create table if not exists run_dataset_profiles (
                run_id varchar primary key,
                profile_name varchar not null,
                description text not null,
                selection_kind varchar not null,
                selection_fingerprint varchar not null,
                selection_json text not null
            )
            """
        )
        connection.execute(
            """
            create index if not exists run_mismatches_by_run_check
            on run_mismatches (run_id, check_id)
            """
        )

    def _register_run(self) -> None:
        """Insert the current run metadata before batch processing starts."""
        connection = self._require_connection()
        connection.execute(
            """
            insert into runs (
                run_id,
                status,
                recorded_at_utc,
                completed_at_utc,
                source_snapshot_id,
                product_count,
                prepare_run_seconds,
                source_snapshot_id_seconds,
                dataset_profile_load_seconds,
                source_row_count_seconds,
                source_db_path,
                requested_check_profile_name,
                active_check_profile_name,
                check_context_provider,
                batch_size,
                batch_workers,
                legacy_backend_workers,
                mismatch_examples_limit,
                reference_result_cache_key,
                reference_result_cache_path,
                requires_reference_results,
                requires_reference_check_contexts,
                requires_reference_findings,
                python_check_count,
                dsl_check_count,
                legacy_parity_check_count,
                runtime_only_check_count,
                requested_dataset_profile_name,
                active_dataset_profile_name,
                active_dataset_selection_kind,
                active_dataset_selection_fingerprint,
                compared_check_count,
                reference_total,
                compared_migrated_total,
                matched_total,
                runtime_only_migrated_total,
                run_artifact_json,
                failure_type,
                failure_message
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                self.prepared_run.run_id,
                "running",
                _utc_now(),
                None,
                self.prepared_run.source_snapshot_id,
                self.prepared_run.product_count,
                self.prepared_run.preparation_timings.prepare_run_seconds,
                self.prepared_run.preparation_timings.source_snapshot_id_seconds,
                self.prepared_run.preparation_timings.dataset_profile_load_seconds,
                self.prepared_run.preparation_timings.source_row_count_seconds,
                str(self.run_spec.db_path),
                self.run_spec.check_profile_name,
                self.prepared_run.active_check_profile.name,
                self.prepared_run.active_check_profile.check_context_provider,
                self.run_spec.batch_size,
                self.run_spec.batch_workers,
                self.run_spec.legacy_backend_workers,
                self.run_spec.mismatch_examples_limit,
                self.prepared_run.reference_result_cache_key,
                (
                    str(self.prepared_run.reference_result_cache_path)
                    if self.prepared_run.reference_result_cache_path is not None
                    else None
                ),
                self.prepared_run.requires_reference_results,
                self.prepared_run.requires_reference_check_contexts,
                self.prepared_run.requires_reference_findings,
                self.prepared_run.python_count,
                self.prepared_run.dsl_count,
                self.prepared_run.legacy_parity_count,
                self.prepared_run.runtime_only_count,
                self.run_spec.dataset_profile_name,
                self.prepared_run.active_dataset_profile.name,
                self.prepared_run.active_dataset_profile.selection.kind,
                self.prepared_run.active_dataset_profile.selection.fingerprint,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        )

    def _snapshot_dataset_profile(self) -> None:
        """Persist the resolved dataset profile for this run."""
        connection = self._require_connection()
        dataset_profile = self.prepared_run.active_dataset_profile
        connection.execute(
            """
            insert into run_dataset_profiles (
                run_id,
                profile_name,
                description,
                selection_kind,
                selection_fingerprint,
                selection_json
            )
            values (?, ?, ?, ?, ?, ?)
            """,
            [
                self.prepared_run.run_id,
                dataset_profile.name,
                dataset_profile.description,
                dataset_profile.selection.kind,
                dataset_profile.selection.fingerprint,
                json.dumps(
                    dataset_profile.selection.as_payload(),
                    ensure_ascii=False,
                ),
            ],
        )

    def _build_mismatch_rows(
        self,
        batch_result: BatchExecutionResult,
    ) -> list[list[object]]:
        """Return store rows for all concrete mismatches in one batch."""
        rows: list[list[object]] = []
        for check_result in batch_result.run_result.checks:
            rows.extend(
                self._mismatch_rows_for_findings(
                    batch_index=batch_result.batch_index,
                    check_result=check_result,
                    mismatch_kind="missing",
                    findings=check_result.missing,
                )
            )
            rows.extend(
                self._mismatch_rows_for_findings(
                    batch_index=batch_result.batch_index,
                    check_result=check_result,
                    mismatch_kind="extra",
                    findings=check_result.extra,
                )
            )
        return rows

    def _mismatch_rows_for_findings(
        self,
        *,
        batch_index: int,
        check_result: RunCheckResult,
        mismatch_kind: str,
        findings: list[ObservedFinding],
    ) -> list[list[object]]:
        """Return store rows for one mismatch side of one batch check."""
        return [
            [
                self.prepared_run.run_id,
                batch_index,
                check_result.definition.id,
                mismatch_kind,
                finding.side,
                finding.product_id,
                finding.observed_code,
                finding.severity,
            ]
            for finding in findings
        ]

    def _check_summary_row(self, check_result: RunCheckResult) -> list[object]:
        """Return the persisted final summary row for one active check."""
        return [
            self.prepared_run.run_id,
            check_result.definition.id,
            check_result.definition.definition_language,
            check_result.definition.parity_baseline,
            check_result.comparison_status,
            check_result.passed,
            check_result.reference_count,
            check_result.migrated_count,
            check_result.matched_count,
            check_result.missing_count,
            check_result.extra_count,
            (
                check_result.definition.legacy_identity.code_template
                if check_result.definition.legacy_identity is not None
                else None
            ),
        ]

    def _mark_run_terminated(
        self,
        *,
        status: str,
        failure_type: str | None = None,
        failure_message: str | None = None,
    ) -> None:
        """Persist a terminal unfinished status before closing the store."""
        connection = self._require_connection()
        connection.execute(
            """
            update runs
            set
                status = ?,
                completed_at_utc = ?,
                failure_type = ?,
                failure_message = ?
            where run_id = ?
            """,
            [
                status,
                _utc_now(),
                failure_type,
                failure_message,
                self.prepared_run.run_id,
            ],
        )

    def _require_connection(self) -> duckdb.DuckDBPyConnection:
        """Return the active store connection or fail fast."""
        if self._connection is None:
            raise RuntimeError("Run recorder has not been started.")
        return self._connection


def _utc_now() -> str:
    """Return the current timestamp in stable UTC string form."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
