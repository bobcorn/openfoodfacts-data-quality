from __future__ import annotations

import csv
import json
from collections.abc import Callable
from pathlib import Path

from export_legacy_inventory import (
    ESTIMATION_SHEET_FILENAME,
    LEGACY_FAMILIES_FILENAME,
    build_legacy_families_artifact,
    export_legacy_inventory,
)


def test_build_legacy_families_artifact_groups_templates_and_features(
    tmp_path: Path,
    legacy_source_root_factory: Callable[[Path], Path],
) -> None:
    legacy_root = legacy_source_root_factory(tmp_path)

    artifact = build_legacy_families_artifact(legacy_source_root=legacy_root)
    families_by_id = {family["check_id"]: family for family in artifact["families"]}

    food_groups_known = families_by_id["en:food-groups-${level}-known"]
    energy_mismatch = families_by_id[
        "en:${set_id}-energy-value-in-${unit}-does-not-match-value-computed-from-other-nutrients"
    ]
    language_mismatch = families_by_id[
        "en:ingredients-language-mismatch-${ingredients_lc}-contains-${lc}"
    ]
    vitamin_claim = families_by_id[
        "en:${vit_or_min_label_no_lc}-label-claim-but-${vit_or_min}-below-${vitamins_and_minerals_labelling_europe_vit_or_min_vit_or_min_label}"
    ]
    specific_ingredient = families_by_id[
        "en:specific-ingredient-${specific_ingredient_id_no_lc}-quantity-is-below-the-minimum-value-of-${quantity_threshold}-for-category-${category_id_no_lc}"
    ]
    incompatible_tags = families_by_id[
        "en:mutually-exclusive-tags-for-${incompatible_tags_0}-and-${incompatible_tags_1}"
    ]

    assert artifact["version"] == 1
    assert artifact["module_paths"] == [
        "lib/ProductOpener/DataQualityCommon.pm",
        "lib/ProductOpener/DataQualityDimensions.pm",
        "lib/ProductOpener/DataQualityFood.pm",
    ]
    assert artifact["source_fingerprint"].startswith("sha256:")
    assert food_groups_known["placeholder_names"] == ["level"]
    assert food_groups_known["features"]["has_loop"] is True
    assert food_groups_known["features"]["has_branching"] is True
    assert food_groups_known["features"]["has_arithmetic"] is True
    assert food_groups_known["features"]["helper_calls"] == ["deep_exists"]
    assert food_groups_known["sources"][0]["source_subroutine"] == "check_food_groups"
    assert "sub check_food_groups" in food_groups_known["sources"][0]["code"]
    assert energy_mismatch["placeholder_names"] == ["set_id", "unit"]
    assert energy_mismatch["features"]["has_loop"] is False
    assert (
        energy_mismatch["sources"][0]["source_file"]
        == "lib/ProductOpener/DataQualityFood.pm"
    )
    assert language_mismatch["placeholder_names"] == ["ingredients_lc", "lc"]
    assert vitamin_claim["placeholder_names"] == [
        "vit_or_min_label_no_lc",
        "vit_or_min",
        "vitamins_and_minerals_labelling_europe_vit_or_min_vit_or_min_label",
    ]
    assert specific_ingredient["placeholder_names"] == [
        "specific_ingredient_id_no_lc",
        "quantity_threshold",
        "category_id_no_lc",
    ]
    assert incompatible_tags["placeholder_names"] == [
        "incompatible_tags_0",
        "incompatible_tags_1",
    ]


def test_export_legacy_inventory_writes_json_and_csv(
    tmp_path: Path,
    legacy_source_root_factory: Callable[[Path], Path],
) -> None:
    legacy_root = legacy_source_root_factory(tmp_path)
    output_dir = tmp_path / "inventory"

    artifact_path, csv_path = export_legacy_inventory(
        legacy_source_root=legacy_root,
        output_dir=output_dir,
    )

    assert artifact_path == output_dir / LEGACY_FAMILIES_FILENAME
    assert csv_path == output_dir / ESTIMATION_SHEET_FILENAME

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    with csv_path.open(encoding="utf-8", newline="") as csv_file:
        rows = list(csv.DictReader(csv_file))

    assert len(rows) == len(artifact["families"])
    assert "source_subroutine" not in rows[0]
    assert rows[0]["target_impl"] == ""
    assert rows[0]["size"] == ""
    assert rows[0]["risk"] == ""
    assert rows[0]["estimated_hours"] == ""
    assert rows[0]["rationale"] == ""
