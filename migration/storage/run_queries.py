from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from migration._value_shapes import is_string_object_mapping
from migration.run.serialization import parse_run_artifact
from off_data_quality.contracts.run import RunResult


@dataclass(frozen=True, slots=True)
class RecordedDatasetProfile:
    """Store-backed dataset metadata for one recorded run."""

    name: str
    description: str
    selection_kind: str
    selection_fingerprint: str


@dataclass(frozen=True, slots=True)
class RecordedRunSnapshot:
    """Store-backed read model used by report and review rendering."""

    run_artifact: dict[str, Any]
    run_result: RunResult
    dataset_profile: RecordedDatasetProfile | None


@dataclass(frozen=True, slots=True)
class RecordedRunBenchmarkSummary:
    """Store-backed timing summary used by local benchmark tooling."""

    run_id: str
    status: str
    product_count: int
    prepare_run_seconds: float
    source_snapshot_id_seconds: float
    dataset_profile_load_seconds: float
    source_row_count_seconds: float
    batch_count: int
    cache_hit_count: int
    backend_run_count: int
    reference_finding_count: int
    migrated_finding_count: int
    batch_elapsed_seconds: float
    source_read_seconds: float
    reference_load_seconds: float
    reference_check_context_materialization_seconds: float
    reference_finding_materialization_seconds: float
    migrated_findings_seconds: float
    parity_compare_seconds: float
    record_batch_seconds: float

    def as_payload(self) -> dict[str, Any]:
        """Return a stable payload that is safe to serialize as JSON."""
        return {
            "run_id": self.run_id,
            "status": self.status,
            "product_count": self.product_count,
            "run_preparation": {
                "prepare_run_seconds": self.prepare_run_seconds,
                "source_snapshot_id_seconds": self.source_snapshot_id_seconds,
                "dataset_profile_load_seconds": self.dataset_profile_load_seconds,
                "source_row_count_seconds": self.source_row_count_seconds,
            },
            "batch_count": self.batch_count,
            "cache_hit_count": self.cache_hit_count,
            "backend_run_count": self.backend_run_count,
            "reference_finding_count": self.reference_finding_count,
            "migrated_finding_count": self.migrated_finding_count,
            "batch_elapsed_seconds": self.batch_elapsed_seconds,
            "record_batch_seconds": self.record_batch_seconds,
            "stage_timings": {
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
            },
        }


def load_recorded_run_snapshot(
    store_path: Path,
    *,
    run_id: str,
) -> RecordedRunSnapshot:
    """Load the completed store-backed snapshot for one recorded run."""
    connection = duckdb.connect(str(store_path), read_only=True)
    try:
        run_row = connection.execute(
            """
            select
                status,
                run_artifact_json
            from runs
            where run_id = ?
            """,
            [run_id],
        ).fetchone()
        if run_row is None:
            raise FileNotFoundError(
                f"Run {run_id!r} not found in parity store {store_path}."
            )

        status, run_artifact_json = run_row
        if status != "completed":
            raise RuntimeError(
                f"Run {run_id!r} in parity store {store_path} is not completed: {status!r}."
            )
        if not isinstance(run_artifact_json, str) or not run_artifact_json:
            raise RuntimeError(
                f"Run {run_id!r} in parity store {store_path} has no persisted run artifact."
            )

        raw_artifact = json.loads(run_artifact_json)
        if not is_string_object_mapping(raw_artifact):
            raise RuntimeError(
                f"Run {run_id!r} in parity store {store_path} has an invalid run artifact payload."
            )

        dataset_profile = _load_dataset_profile(connection, run_id=run_id)
        run_artifact: dict[str, Any] = dict(raw_artifact)
        return RecordedRunSnapshot(
            run_artifact=run_artifact,
            run_result=parse_run_artifact(run_artifact),
            dataset_profile=dataset_profile,
        )
    finally:
        connection.close()


def load_recorded_run_benchmark_summary(
    store_path: Path,
    *,
    run_id: str,
) -> RecordedRunBenchmarkSummary:
    """Load aggregated batch timings for one recorded run."""
    connection = duckdb.connect(str(store_path), read_only=True)
    try:
        run_row = connection.execute(
            """
            select
                status,
                product_count,
                prepare_run_seconds,
                source_snapshot_id_seconds,
                dataset_profile_load_seconds,
                source_row_count_seconds
            from runs
            where run_id = ?
            """,
            [run_id],
        ).fetchone()
        if run_row is None:
            raise FileNotFoundError(
                f"Run {run_id!r} not found in parity store {store_path}."
            )

        batch_row = connection.execute(
            """
            select
                count(*),
                coalesce(sum(cache_hit_count), 0),
                coalesce(sum(backend_run_count), 0),
                coalesce(sum(reference_finding_count), 0),
                coalesce(sum(migrated_finding_count), 0),
                coalesce(sum(elapsed_seconds), 0.0),
                coalesce(sum(source_read_seconds), 0.0),
                coalesce(sum(reference_load_seconds), 0.0),
                coalesce(sum(reference_check_context_materialization_seconds), 0.0),
                coalesce(sum(reference_finding_materialization_seconds), 0.0),
                coalesce(sum(migrated_findings_seconds), 0.0),
                coalesce(sum(parity_compare_seconds), 0.0),
                coalesce(sum(record_batch_seconds), 0.0)
            from run_batches
            where run_id = ?
            """,
            [run_id],
        ).fetchone()
        if batch_row is None:
            raise RuntimeError(
                f"Run {run_id!r} in parity store {store_path} has no batch summary."
            )

        return RecordedRunBenchmarkSummary(
            run_id=run_id,
            status=str(run_row[0]),
            product_count=int(run_row[1]),
            prepare_run_seconds=float(run_row[2]),
            source_snapshot_id_seconds=float(run_row[3]),
            dataset_profile_load_seconds=float(run_row[4]),
            source_row_count_seconds=float(run_row[5]),
            batch_count=int(batch_row[0]),
            cache_hit_count=int(batch_row[1]),
            backend_run_count=int(batch_row[2]),
            reference_finding_count=int(batch_row[3]),
            migrated_finding_count=int(batch_row[4]),
            batch_elapsed_seconds=float(batch_row[5]),
            source_read_seconds=float(batch_row[6]),
            reference_load_seconds=float(batch_row[7]),
            reference_check_context_materialization_seconds=float(batch_row[8]),
            reference_finding_materialization_seconds=float(batch_row[9]),
            migrated_findings_seconds=float(batch_row[10]),
            parity_compare_seconds=float(batch_row[11]),
            record_batch_seconds=float(batch_row[12]),
        )
    finally:
        connection.close()


def _load_dataset_profile(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
) -> RecordedDatasetProfile | None:
    """Return the persisted dataset profile for one recorded run."""
    row = connection.execute(
        """
        select
            profile_name,
            description,
            selection_kind,
            selection_fingerprint,
            selection_json
        from run_dataset_profiles
        where run_id = ?
        """,
        [run_id],
    ).fetchone()
    if row is None:
        return None
    (
        profile_name,
        description,
        selection_kind,
        selection_fingerprint,
        selection_json,
    ) = row
    payload = json.loads(str(selection_json))
    if not is_string_object_mapping(payload):
        raise RuntimeError(
            f"Run {run_id!r} has an invalid dataset selection payload in the run store."
        )
    return RecordedDatasetProfile(
        name=str(profile_name),
        description=str(description),
        selection_kind=str(selection_kind),
        selection_fingerprint=str(selection_fingerprint),
    )
