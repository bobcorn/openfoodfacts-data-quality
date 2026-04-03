from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

from app.migration.catalog import load_migration_catalog


def test_load_migration_catalog_joins_inventory_and_estimation_sheet(
    tmp_path: Path,
    migration_inventory_factory: Callable[[Path], tuple[Path, Path]],
) -> None:
    artifact_path, estimation_path = migration_inventory_factory(tmp_path)
    estimation_path.write_text(
        "\n".join(
            [
                "check_id,target_impl,size,risk,estimated_hours,rationale",
                "en:check-a,dsl,S,low,1,Small DSL migration.",
                "en:check-b,,,,,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    artifact_payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact_payload["families"][0]["check_id"] = "en:check-a"
    artifact_payload["families"][0]["template_key"] = "en:check-a"
    artifact_payload["families"][0]["code_templates"] = ["en:check-a"]
    artifact_payload["families"][0]["features"]["has_branching"] = True
    artifact_payload["families"][0]["features"]["helper_calls"] = ["helper_a"]
    artifact_payload["families"][0]["features"]["statement_count_max"] = 5
    artifact_payload["families"][1]["check_id"] = "en:check-b"
    artifact_payload["families"][1]["template_key"] = "en:check-b"
    artifact_payload["families"][1]["code_templates"] = ["en:check-b"]
    artifact_payload["families"][1]["placeholder_names"] = ["level"]
    artifact_payload["families"][1]["placeholder_count"] = 1
    artifact_payload["families"][1]["features"]["has_loop"] = True
    artifact_payload["families"][1]["features"]["has_arithmetic"] = True
    artifact_payload["families"][1]["features"]["source_files_count"] = 2
    artifact_payload["families"][1]["features"]["source_subroutines_count"] = 2
    artifact_payload["families"][1]["features"][
        "unsupported_data_quality_emission_count_total"
    ] = 1
    artifact_payload["families"][1]["features"]["line_span_max"] = 40
    artifact_payload["families"][1]["features"]["statement_count_max"] = 9
    artifact_path.write_text(
        json.dumps(artifact_payload, ensure_ascii=False),
        encoding="utf-8",
    )

    catalog = load_migration_catalog(
        artifact_path=artifact_path,
        estimation_sheet_path=estimation_path,
    )
    active_plan = catalog.active_plan_for_check_ids(
        ("en:check-a", "en:missing-check", "en:check-b")
    )

    assert catalog.family_count == 2
    assert catalog.assessed_family_count == 1
    assert active_plan.family_count == 2
    assert active_plan.assessed_family_count == 1
    assert active_plan.missing_check_ids == ("en:missing-check",)
    assert active_plan.families[0].target_impl == "dsl"
    assert active_plan.families[0].is_assessed is True
    assert active_plan.families[1].is_assessed is False


def test_load_migration_catalog_treats_missing_artifact_as_unconfigured() -> None:
    catalog = load_migration_catalog(
        artifact_path=None,
        estimation_sheet_path=None,
    )

    active_plan = catalog.active_plan_for_check_ids(
        ("en:check-a", "en:check-b"),
    )

    assert catalog.family_count == 0
    assert active_plan.family_count == 0
    assert active_plan.assessed_family_count == 0
    assert active_plan.missing_check_ids == ()
