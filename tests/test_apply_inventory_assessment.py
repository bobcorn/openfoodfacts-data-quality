from __future__ import annotations

import csv
import json
from pathlib import Path

from apply_inventory_assessment import apply_inventory_assessment

_CSV_FIELDNAMES = (
    "check_id",
    "source_file",
    "line_start",
    "line_end",
    "target_impl",
    "size",
    "risk",
    "estimated_hours",
    "rationale",
)


def _write_artifact(
    path: Path,
    *,
    fingerprint: str,
    check_ids: list[str],
) -> None:
    path.write_text(
        json.dumps(
            {
                "source_fingerprint": fingerprint,
                "families": [{"check_id": check_id} for check_id in check_ids],
            }
        ),
        encoding="utf-8",
    )


def _write_assessment(
    path: Path,
    *,
    fingerprint: str,
    assessments: list[dict[str, object]],
) -> None:
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "legacy_source_fingerprint": fingerprint,
                "assessments": assessments,
            }
        ),
        encoding="utf-8",
    )


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(_CSV_FIELDNAMES)
        for row in rows:
            writer.writerow([row[field] for field in _CSV_FIELDNAMES])


def _csv_row(
    check_id: str,
    *,
    line_start: str,
    line_end: str,
    target_impl: str = "",
    size: str = "",
    risk: str = "",
    estimated_hours: str = "",
    rationale: str = "",
) -> dict[str, str]:
    return {
        "check_id": check_id,
        "source_file": "lib/ProductOpener/DataQualityFood.pm",
        "line_start": line_start,
        "line_end": line_end,
        "target_impl": target_impl,
        "size": size,
        "risk": risk,
        "estimated_hours": estimated_hours,
        "rationale": rationale,
    }


def test_apply_inventory_assessment_fills_blank_fields_and_preserves_values(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "legacy_families.json"
    _write_artifact(
        artifact_path,
        fingerprint="sha256:test",
        check_ids=["en:first-check", "en:second-check"],
    )

    assessment_path = tmp_path / "assessment.json"
    _write_assessment(
        assessment_path,
        fingerprint="sha256:test",
        assessments=[
            {
                "check_id": "en:first-check",
                "target_impl": "dsl",
                "size": "S",
                "risk": "low",
                "rationale": "Single boolean rule with no helper calls or arithmetic, so the current DSL should cover it cleanly.",
            },
            {
                "check_id": "en:second-check",
                "target_impl": "python",
                "size": "L",
                "risk": "high",
                "rationale": "The snippet relies on helper-heavy imperative logic, which pushes it out of the DSL and raises implementation risk.",
            },
        ],
    )

    csv_path = tmp_path / "estimation_sheet.csv"
    _write_csv(
        csv_path,
        rows=[
            _csv_row("en:first-check", line_start="10", line_end="20"),
            _csv_row(
                "en:second-check",
                line_start="30",
                line_end="40",
                target_impl="python",
            ),
        ],
    )

    summary = apply_inventory_assessment(
        assessment_path=assessment_path,
        artifact_path=artifact_path,
        csv_path=csv_path,
        output_csv=csv_path,
    )

    with csv_path.open(encoding="utf-8", newline="") as csv_file:
        rows = list(csv.DictReader(csv_file))

    assert summary.updated_rows == 2
    assert summary.rows_with_missing_fields == 0
    assert rows[0]["target_impl"] == "dsl"
    assert rows[0]["size"] == "S"
    assert rows[0]["estimated_hours"] == ""
    assert rows[1]["target_impl"] == "python"
    assert rows[1]["size"] == "L"
    assert rows[1]["risk"] == "high"


def test_apply_inventory_assessment_rejects_unknown_check_ids(tmp_path: Path) -> None:
    artifact_path = tmp_path / "legacy_families.json"
    _write_artifact(
        artifact_path, fingerprint="sha256:test", check_ids=["en:first-check"]
    )

    assessment_path = tmp_path / "assessment.json"
    _write_assessment(
        assessment_path,
        fingerprint="sha256:test",
        assessments=[
            {
                "check_id": "en:missing-check",
                "target_impl": "dsl",
                "size": "S",
                "risk": "low",
                "rationale": "Irrelevant.",
            }
        ],
    )

    csv_path = tmp_path / "estimation_sheet.csv"
    _write_csv(
        csv_path,
        rows=[_csv_row("en:first-check", line_start="10", line_end="20")],
    )

    try:
        apply_inventory_assessment(
            assessment_path=assessment_path,
            artifact_path=artifact_path,
            csv_path=csv_path,
            output_csv=csv_path,
        )
    except RuntimeError as exc:
        assert "unknown check_id" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected unknown assessment check_id to fail")
