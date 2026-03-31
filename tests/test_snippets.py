from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from app.report.legacy_source import LegacySourceIndex, resolve_legacy_module_paths
from app.report.snippets import build_code_snippet_panels, build_snippet_artifact

from openfoodfacts_data_quality.checks.catalog import CheckCatalog


def test_build_code_snippet_panels_renders_from_structured_snippet_artifact() -> None:
    snippet_artifact = {
        "checks": {
            "en:test-check": [
                {
                    "check_id": "en:test-check",
                    "origin": "migrated",
                    "definition_language": "python",
                    "path": "src/example.py",
                    "start_line": 10,
                    "end_line": 12,
                    "code": "def example() -> None:\n    return None",
                }
            ]
        }
    }

    panels_by_check = build_code_snippet_panels(snippet_artifact)

    assert list(panels_by_check) == ["en:test-check"]
    assert panels_by_check["en:test-check"][0]["title"] == "Migrated Snippet"
    assert "def" in panels_by_check["en:test-check"][0]["html"]


def test_build_snippet_artifact_is_machine_readable(
    tmp_path: Path,
    default_check_catalog: CheckCatalog,
    legacy_source_root_factory: Callable[[Path], Path],
) -> None:
    legacy_root = legacy_source_root_factory(tmp_path)
    snippet_artifact = build_snippet_artifact(
        {"en:food-groups-${level}-known"},
        catalog=default_check_catalog,
        legacy_source_root=legacy_root,
    )

    snippets = snippet_artifact["checks"]["en:food-groups-${level}-known"]
    migrated_snippet = next(
        snippet for snippet in snippets if snippet["origin"] == "migrated"
    )

    assert migrated_snippet["definition_language"] == "python"
    assert (
        migrated_snippet["path"]
        == "src/openfoodfacts_data_quality/checks/packs/python/global_checks.py"
    )
    assert isinstance(migrated_snippet["start_line"], int)
    assert isinstance(migrated_snippet["end_line"], int)
    assert "html" not in migrated_snippet


def test_legacy_source_index_parses_concatenated_and_multiline_templates(
    tmp_path: Path,
    default_check_catalog: CheckCatalog,
    legacy_source_root_factory: Callable[[Path], Path],
) -> None:
    legacy_root = legacy_source_root_factory(tmp_path)
    index = LegacySourceIndex.build(resolve_legacy_module_paths(legacy_root))

    food_group_identity = default_check_catalog.check_by_id(
        "en:food-groups-${level}-known"
    ).legacy_identity
    energy_identity = default_check_catalog.check_by_id(
        "en:${set_id}-energy-value-in-${unit}-does-not-match-value-computed-from-other-nutrients"
    ).legacy_identity
    assert food_group_identity is not None
    assert energy_identity is not None

    food_group_matches = index.matches_for_identity(food_group_identity)
    energy_matches = index.matches_for_identity(energy_identity)

    assert len(food_group_matches) == 1
    assert food_group_matches[0].subroutine_name == "check_food_groups"
    assert "en:food-groups-' . $level . '-known" in food_group_matches[0].code

    assert len(energy_matches) == 1
    assert energy_matches[0].subroutine_name == "check_energy_mismatch"
    assert "en:${set_id}-energy-value-in-$unit" in energy_matches[0].code


def test_build_snippet_artifact_includes_legacy_food_group_and_multiline_snippets(
    tmp_path: Path,
    default_check_catalog: CheckCatalog,
    legacy_source_root_factory: Callable[[Path], Path],
) -> None:
    legacy_root = legacy_source_root_factory(tmp_path)

    snippet_artifact = build_snippet_artifact(
        {
            "en:food-groups-${level}-known",
            "en:food-groups-${level}-unknown",
            "en:${set_id}-energy-value-in-${unit}-does-not-match-value-computed-from-other-nutrients",
            "en:${set_id}-sugars-plus-starch-plus-fiber-greater-than-carbohydrates-total",
        },
        catalog=default_check_catalog,
        legacy_source_root=legacy_root,
    )

    known_snippets = snippet_artifact["checks"]["en:food-groups-${level}-known"]
    energy_snippets = snippet_artifact["checks"][
        "en:${set_id}-energy-value-in-${unit}-does-not-match-value-computed-from-other-nutrients"
    ]

    assert {snippet["origin"] for snippet in known_snippets} == {"migrated", "legacy"}
    assert {snippet["origin"] for snippet in energy_snippets} == {
        "migrated",
        "legacy",
    }

    known_legacy_snippet = next(
        snippet for snippet in known_snippets if snippet["origin"] == "legacy"
    )
    energy_legacy_snippet = next(
        snippet for snippet in energy_snippets if snippet["origin"] == "legacy"
    )

    assert known_legacy_snippet["path"] == "lib/ProductOpener/DataQualityFood.pm"
    assert "sub check_food_groups" in known_legacy_snippet["code"]
    assert "sub check_energy_mismatch" in energy_legacy_snippet["code"]


def test_build_snippet_artifact_requires_legacy_source_for_legacy_checks(
    tmp_path: Path,
    default_check_catalog: CheckCatalog,
) -> None:
    with pytest.raises(RuntimeError, match="Legacy snippet extraction requires"):
        build_snippet_artifact(
            {"en:serving-quantity-over-product-quantity"},
            catalog=default_check_catalog,
            legacy_source_root=tmp_path / "missing-legacy-root",
        )
