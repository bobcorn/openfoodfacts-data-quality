from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import duckdb

from app.migration.catalog import MigrationAssessment, MigrationFamily
from app.run.serialization import parse_run_artifact
from openfoodfacts_data_quality.contracts.run import RunResult
from openfoodfacts_data_quality.structured_values import is_string_object_mapping


@dataclass(frozen=True, slots=True)
class RecordedDatasetProfile:
    """Store-backed dataset metadata for one recorded run."""

    name: str
    description: str
    selection_kind: str
    selection_fingerprint: str


@dataclass(frozen=True, slots=True)
class CheckMismatchGovernanceSummary:
    """Per-check governance counts loaded from the run store."""

    expected_missing_count: int = 0
    unexpected_missing_count: int = 0
    expected_extra_count: int = 0
    unexpected_extra_count: int = 0

    @property
    def expected_total_mismatches(self) -> int:
        """Return the number of governed mismatches for this check."""
        return self.expected_missing_count + self.expected_extra_count

    @property
    def unexpected_total_mismatches(self) -> int:
        """Return the number of still-unexpected mismatches for this check."""
        return self.unexpected_missing_count + self.unexpected_extra_count


@dataclass(frozen=True, slots=True)
class RecordedRunSnapshot:
    """Store-backed read model used by report and review rendering."""

    run_artifact: dict[str, Any]
    run_result: RunResult
    dataset_profile: RecordedDatasetProfile | None
    expected_differences_rule_count: int
    check_governance_by_id: dict[str, CheckMismatchGovernanceSummary]
    migration_families_by_check_id: dict[str, MigrationFamily]
    active_migration_family_count: int
    assessed_migration_family_count: int
    unmatched_migration_check_count: int

    @property
    def expected_mismatch_total(self) -> int:
        """Return the run-wide number of governed mismatches."""
        return sum(
            counts.expected_total_mismatches
            for counts in self.check_governance_by_id.values()
        )

    @property
    def unexpected_mismatch_total(self) -> int:
        """Return the run-wide number of still-unexpected mismatches."""
        return sum(
            counts.unexpected_total_mismatches
            for counts in self.check_governance_by_id.values()
        )


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
                run_artifact_json,
                expected_differences_rule_count
            from runs
            where run_id = ?
            """,
            [run_id],
        ).fetchone()
        if run_row is None:
            raise FileNotFoundError(
                f"Run {run_id!r} not found in parity store {store_path}."
            )

        status, run_artifact_json, rule_count = run_row
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

        check_governance_by_id = _load_check_governance_by_id(connection, run_id=run_id)
        dataset_profile = _load_dataset_profile(connection, run_id=run_id)
        (
            migration_families_by_check_id,
            active_migration_family_count,
            assessed_migration_family_count,
            unmatched_migration_check_count,
        ) = _load_migration_snapshot(connection, run_id=run_id)
        run_artifact: dict[str, Any] = dict(raw_artifact)
        return RecordedRunSnapshot(
            run_artifact=run_artifact,
            run_result=parse_run_artifact(run_artifact),
            dataset_profile=dataset_profile,
            expected_differences_rule_count=int(rule_count),
            check_governance_by_id=check_governance_by_id,
            migration_families_by_check_id=migration_families_by_check_id,
            active_migration_family_count=active_migration_family_count,
            assessed_migration_family_count=assessed_migration_family_count,
            unmatched_migration_check_count=unmatched_migration_check_count,
        )
    finally:
        connection.close()


def _load_check_governance_by_id(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
) -> dict[str, CheckMismatchGovernanceSummary]:
    """Return the per-check governance counts stored for one run."""
    rows = connection.execute(
        """
        select
            check_id,
            expected_missing_count,
            unexpected_missing_count,
            expected_extra_count,
            unexpected_extra_count
        from run_check_summaries
        where run_id = ?
        """,
        [run_id],
    ).fetchall()
    return {
        str(check_id): CheckMismatchGovernanceSummary(
            expected_missing_count=int(expected_missing_count),
            unexpected_missing_count=int(unexpected_missing_count),
            expected_extra_count=int(expected_extra_count),
            unexpected_extra_count=int(unexpected_extra_count),
        )
        for (
            check_id,
            expected_missing_count,
            unexpected_missing_count,
            expected_extra_count,
            unexpected_extra_count,
        ) in rows
    }


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


def _load_migration_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
) -> tuple[dict[str, MigrationFamily], int, int, int]:
    """Return the store-backed migration metadata for one recorded run."""
    counts_row = connection.execute(
        """
        select
            coalesce(active_migration_family_count, 0),
            coalesce(assessed_migration_family_count, 0),
            coalesce(unmatched_migration_check_count, 0)
        from runs
        where run_id = ?
        """,
        [run_id],
    ).fetchone()
    if counts_row is None:
        return {}, 0, 0, 0

    rows = connection.execute(
        """
        select
            check_id,
            template_key,
            code_templates_json,
            placeholder_names_json,
            placeholder_count,
            has_loop,
            has_branching,
            has_arithmetic,
            helper_calls_json,
            source_files_count,
            source_subroutines_count,
            unsupported_data_quality_emission_count_total,
            line_span_max,
            statement_count_max,
            target_impl,
            size,
            risk,
            estimated_hours,
            rationale,
            is_assessed
        from run_active_migration_families
        where run_id = ?
        """,
        [run_id],
    ).fetchall()
    return (
        {
            str(check_id): MigrationFamily(
                check_id=str(check_id),
                template_key=str(template_key),
                code_templates=tuple(json.loads(str(code_templates_json))),
                placeholder_names=tuple(json.loads(str(placeholder_names_json))),
                placeholder_count=int(placeholder_count),
                has_loop=bool(has_loop),
                has_branching=bool(has_branching),
                has_arithmetic=bool(has_arithmetic),
                helper_calls=tuple(json.loads(str(helper_calls_json))),
                source_files_count=int(source_files_count),
                source_subroutines_count=int(source_subroutines_count),
                unsupported_data_quality_emission_count_total=int(
                    unsupported_data_quality_emission_count_total
                ),
                line_span_max=int(line_span_max),
                statement_count_max=int(statement_count_max),
                assessment=MigrationAssessment(
                    target_impl=cast(
                        "Literal['dsl', 'python'] | None",
                        str(target_impl) if target_impl is not None else None,
                    ),
                    size=cast(
                        "Literal['S', 'M', 'L'] | None",
                        str(size) if size is not None else None,
                    ),
                    risk=cast(
                        "Literal['low', 'medium', 'high'] | None",
                        str(risk) if risk is not None else None,
                    ),
                    estimated_hours=(
                        str(estimated_hours) if estimated_hours is not None else None
                    ),
                    rationale=str(rationale) if rationale is not None else None,
                ),
            )
            for (
                check_id,
                template_key,
                code_templates_json,
                placeholder_names_json,
                placeholder_count,
                has_loop,
                has_branching,
                has_arithmetic,
                helper_calls_json,
                source_files_count,
                source_subroutines_count,
                unsupported_data_quality_emission_count_total,
                line_span_max,
                statement_count_max,
                target_impl,
                size,
                risk,
                estimated_hours,
                rationale,
                _unused_is_assessed,
            ) in rows
        },
        int(counts_row[0]),
        int(counts_row[1]),
        int(counts_row[2]),
    )
