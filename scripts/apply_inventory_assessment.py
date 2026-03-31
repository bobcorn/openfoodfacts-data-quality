from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeGuard, cast

DEFAULT_INVENTORY_DIR = (
    Path(__file__).resolve().parents[1] / "artifacts" / "legacy_inventory"
)
DEFAULT_ASSESSMENT_PATH = DEFAULT_INVENTORY_DIR / "assessment.json"
DEFAULT_ARTIFACT_PATH = DEFAULT_INVENTORY_DIR / "legacy_families.json"
DEFAULT_CSV_PATH = DEFAULT_INVENTORY_DIR / "estimation_sheet.csv"
_PLANNING_FIELDS = (
    "target_impl",
    "size",
    "risk",
    "rationale",
)
_TARGET_IMPL_VALUES = frozenset({"dsl", "python"})
_SIZE_VALUES = frozenset({"S", "M", "L"})
_RISK_VALUES = frozenset({"low", "medium", "high"})


@dataclass(frozen=True, slots=True)
class AssessmentEntry:
    check_id: str
    target_impl: str
    size: str
    risk: str
    rationale: str

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> AssessmentEntry:
        check_id = _require_non_empty_string(payload, "check_id")
        target_impl = _require_enum_string(
            payload,
            "target_impl",
            allowed_values=_TARGET_IMPL_VALUES,
        )
        size = _require_enum_string(payload, "size", allowed_values=_SIZE_VALUES)
        risk = _require_enum_string(payload, "risk", allowed_values=_RISK_VALUES)
        rationale = _require_non_empty_string(payload, "rationale")
        return cls(
            check_id=check_id,
            target_impl=target_impl,
            size=size,
            risk=risk,
            rationale=rationale,
        )


@dataclass(frozen=True, slots=True)
class AssessmentDocument:
    legacy_source_fingerprint: str
    assessments_by_check_id: dict[str, AssessmentEntry]

    @classmethod
    def load(cls, path: Path) -> AssessmentDocument:
        payload = _require_object(
            json.loads(path.read_text(encoding="utf-8")),
            context=str(path),
        )
        if payload.get("version") != 1:
            raise RuntimeError(f"{path} must declare version=1.")

        legacy_source_fingerprint = _require_non_empty_string(
            payload,
            "legacy_source_fingerprint",
        )
        assessments_payload = _require_list(
            payload.get("assessments"),
            context=f"{path} assessments",
        )
        if not assessments_payload:
            raise RuntimeError(f"{path} must contain a non-empty assessments array.")

        assessments_by_check_id: dict[str, AssessmentEntry] = {}
        for raw_entry in assessments_payload:
            entry = AssessmentEntry.from_json(
                _require_object(raw_entry, context=f"{path} assessment entry")
            )
            if entry.check_id in assessments_by_check_id:
                raise RuntimeError(
                    f"{path} contains duplicate assessment entries for {entry.check_id}."
                )
            assessments_by_check_id[entry.check_id] = entry

        return cls(
            legacy_source_fingerprint=legacy_source_fingerprint,
            assessments_by_check_id=assessments_by_check_id,
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fill estimation_sheet.csv planning fields from assessment.json while "
            "preserving existing non-empty CSV values by default."
        )
    )
    parser.add_argument(
        "--assessment-path",
        type=Path,
        default=DEFAULT_ASSESSMENT_PATH,
        help="Path to assessment.json.",
    )
    parser.add_argument(
        "--artifact-path",
        type=Path,
        default=DEFAULT_ARTIFACT_PATH,
        help="Path to legacy_families.json for fingerprint and check-id validation.",
    )
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=DEFAULT_CSV_PATH,
        help="Path to the estimation CSV to update.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Optional output path. Defaults to overwriting --csv-path in place.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite already populated planning cells instead of filling only blanks.",
    )
    args = parser.parse_args()

    output_csv = args.output_csv or args.csv_path
    summary = apply_inventory_assessment(
        assessment_path=args.assessment_path,
        artifact_path=args.artifact_path,
        csv_path=args.csv_path,
        output_csv=output_csv,
        overwrite=args.overwrite,
    )
    print(
        (
            f"[OK] updated {summary.updated_rows} row(s), left "
            f"{summary.rows_with_missing_fields} row(s) with at least one blank "
            f"assessment-driven field, wrote {output_csv}"
        ),
        flush=True,
    )
    return 0


@dataclass(frozen=True, slots=True)
class ApplySummary:
    updated_rows: int
    rows_with_missing_fields: int


def apply_inventory_assessment(
    *,
    assessment_path: Path,
    artifact_path: Path,
    csv_path: Path,
    output_csv: Path,
    overwrite: bool = False,
) -> ApplySummary:
    assessment = AssessmentDocument.load(assessment_path)
    artifact = _require_object(
        json.loads(artifact_path.read_text(encoding="utf-8")),
        context=str(artifact_path),
    )
    artifact_fingerprint = artifact.get("source_fingerprint")
    if artifact_fingerprint != assessment.legacy_source_fingerprint:
        raise RuntimeError(
            "assessment.json legacy_source_fingerprint does not match "
            "legacy_families.json source_fingerprint."
        )

    valid_check_ids = [family["check_id"] for family in artifact.get("families", [])]
    if not valid_check_ids:
        raise RuntimeError(f"{artifact_path} does not contain any families.")
    valid_check_id_set = set(valid_check_ids)

    unknown_assessment_ids = sorted(
        set(assessment.assessments_by_check_id) - valid_check_id_set
    )
    if unknown_assessment_ids:
        raise RuntimeError(
            "assessment.json contains unknown check_id values: "
            + ", ".join(unknown_assessment_ids)
        )

    with csv_path.open(encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        fieldnames = tuple(reader.fieldnames or ())
        _validate_csv_columns(fieldnames, csv_path)
        rows = list(reader)

    csv_check_ids = [row.get("check_id", "") for row in rows]
    duplicate_csv_ids = sorted(_duplicates(csv_check_ids))
    if duplicate_csv_ids:
        raise RuntimeError(
            f"{csv_path} contains duplicate check_id rows: {', '.join(duplicate_csv_ids)}"
        )

    updated_rows = 0
    rows_with_missing_fields = 0
    for row in rows:
        check_id = row["check_id"]
        assessment_entry = assessment.assessments_by_check_id.get(check_id)
        if assessment_entry is not None:
            row_updated = _apply_assessment_entry(
                row,
                assessment_entry=assessment_entry,
                overwrite=overwrite,
            )
            if row_updated:
                updated_rows += 1

        if any(not str(row.get(field, "")).strip() for field in _PLANNING_FIELDS):
            rows_with_missing_fields += 1

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return ApplySummary(
        updated_rows=updated_rows,
        rows_with_missing_fields=rows_with_missing_fields,
    )


def _apply_assessment_entry(
    row: dict[str, str],
    *,
    assessment_entry: AssessmentEntry,
    overwrite: bool,
) -> bool:
    updated = False
    updates = {
        "target_impl": assessment_entry.target_impl,
        "size": assessment_entry.size,
        "risk": assessment_entry.risk,
        "rationale": assessment_entry.rationale,
    }
    for field, value in updates.items():
        current_value = str(row.get(field, "")).strip()
        if overwrite or not current_value:
            if row.get(field) != value:
                row[field] = value
                updated = True
    return updated


def _validate_csv_columns(fieldnames: tuple[str, ...], csv_path: Path) -> None:
    required_columns = {"check_id", *(_PLANNING_FIELDS), "estimated_hours"}
    missing_columns = sorted(required_columns - set(fieldnames))
    if missing_columns:
        raise RuntimeError(
            f"{csv_path} is missing required columns: {', '.join(missing_columns)}"
        )


def _duplicates(values: list[str]) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return duplicates


def _require_non_empty_string(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(
            f"assessment field {field_name!r} must be a non-empty string."
        )
    return value.strip()


def _require_enum_string(
    payload: dict[str, Any],
    field_name: str,
    *,
    allowed_values: frozenset[str],
) -> str:
    value = _require_non_empty_string(payload, field_name)
    if value not in allowed_values:
        raise RuntimeError(
            f"assessment field {field_name!r} must be one of "
            f"{', '.join(sorted(allowed_values))}."
        )
    return value


def _require_object(value: Any, *, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f"{context} must be a JSON object.")
    return cast(dict[str, Any], value)


def _require_list(value: Any, *, context: str) -> list[Any]:
    if not _is_any_list(value):
        raise RuntimeError(f"{context} must be a JSON array.")
    return value


def _is_any_list(value: Any) -> TypeGuard[list[Any]]:
    return isinstance(value, list)


if __name__ == "__main__":
    raise SystemExit(main())
