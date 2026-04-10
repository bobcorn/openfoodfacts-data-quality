from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import cast

import migration.legacy_source as legacy_source_module
import pytest
from migration.legacy_source import (
    LegacySourceIndex,
    resolve_legacy_module_paths,
    resolve_legacy_source_root,
)
from migration.report.snippets import (
    SNIPPETS_ARTIFACT_KIND,
    SNIPPETS_ARTIFACT_SCHEMA_VERSION,
    SnippetArtifact,
    SnippetChecks,
    build_code_snippet_panels,
    build_snippet_artifact,
)

from off_data_quality.catalog import CheckCatalog


def test_build_code_snippet_panels_renders_from_structured_snippet_artifact() -> None:
    snippet_artifact: SnippetArtifact = {
        "checks": {
            "en:test-check": {
                "legacy_snippet_status": "not_applicable",
                "snippets": [
                    {
                        "check_id": "en:test-check",
                        "origin": "implementation",
                        "definition_language": "python",
                        "path": "src/example.py",
                        "start_line": 10,
                        "end_line": 12,
                        "code": "def example() -> None:\n    return None",
                    }
                ],
            }
        }
    }

    panels_by_check = build_code_snippet_panels(snippet_artifact)

    assert list(panels_by_check) == ["en:test-check"]
    assert panels_by_check["en:test-check"][0]["title"] == "Current Implementation"
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

    assert snippet_artifact["kind"] == SNIPPETS_ARTIFACT_KIND
    assert snippet_artifact["schema_version"] == SNIPPETS_ARTIFACT_SCHEMA_VERSION
    assert snippet_artifact["issues"] == []

    snippets = cast(
        SnippetChecks,
        snippet_artifact["checks"],
    )["en:food-groups-${level}-known"]
    assert snippets["legacy_snippet_status"] == "available"
    snippet_list = cast(list[dict[str, object]], snippets["snippets"])
    implementation_snippet = next(
        snippet for snippet in snippet_list if snippet["origin"] == "implementation"
    )

    assert implementation_snippet["definition_language"] == "python"
    assert (
        implementation_snippet["path"]
        == "off_data_quality/checks/packs/python/global_checks.py"
    )
    assert isinstance(implementation_snippet["start_line"], int)
    assert isinstance(implementation_snippet["end_line"], int)
    assert "html" not in implementation_snippet


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

    checks_by_id = cast(SnippetChecks, snippet_artifact["checks"])
    known_entry = checks_by_id["en:food-groups-${level}-known"]
    energy_entry = checks_by_id[
        "en:${set_id}-energy-value-in-${unit}-does-not-match-value-computed-from-other-nutrients"
    ]
    known_snippets = cast(list[dict[str, object]], known_entry["snippets"])
    energy_snippets = cast(list[dict[str, object]], energy_entry["snippets"])

    assert known_entry["legacy_snippet_status"] == "available"
    assert energy_entry["legacy_snippet_status"] == "available"

    assert {snippet["origin"] for snippet in known_snippets} == {
        "implementation",
        "legacy",
    }
    assert {snippet["origin"] for snippet in energy_snippets} == {
        "implementation",
        "legacy",
    }

    known_legacy_snippet = next(
        snippet for snippet in known_snippets if snippet["origin"] == "legacy"
    )
    energy_legacy_snippet = next(
        snippet for snippet in energy_snippets if snippet["origin"] == "legacy"
    )

    assert known_legacy_snippet["path"] == "lib/ProductOpener/DataQualityFood.pm"
    assert "sub check_food_groups" in str(known_legacy_snippet["code"])
    assert "sub check_energy_mismatch" in str(energy_legacy_snippet["code"])


def test_build_snippet_artifact_degrades_without_legacy_source_for_legacy_checks(
    tmp_path: Path,
    default_check_catalog: CheckCatalog,
) -> None:
    snippet_artifact = build_snippet_artifact(
        {"en:serving-quantity-over-product-quantity"},
        catalog=default_check_catalog,
        legacy_source_root=tmp_path / "missing-legacy-root",
    )

    issues = cast(list[dict[str, object]], snippet_artifact["issues"])
    snippets = cast(
        SnippetChecks,
        snippet_artifact["checks"],
    )["en:serving-quantity-over-product-quantity"]
    assert snippets["legacy_snippet_status"] == "unavailable"
    snippet_list = cast(list[dict[str, object]], snippets["snippets"])

    assert len(issues) == 1
    assert issues[0]["severity"] == "warning"
    assert issues[0]["check_ids"] == ["en:serving-quantity-over-product-quantity"]
    assert {snippet["origin"] for snippet in snippet_list} == {"implementation"}


def test_resolve_legacy_source_root_finds_sibling_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = tmp_path / "workspace" / "openfoodfacts-data-quality"
    app_dir = project_root / "app"
    fake_module_path = app_dir / "legacy_source.py"
    fake_module_path.parent.mkdir(parents=True)
    fake_module_path.write_text("# stub\n", encoding="utf-8")

    legacy_root = project_root.parent / "openfoodfacts-server"
    product_opener_dir = legacy_root / "lib" / "ProductOpener"
    product_opener_dir.mkdir(parents=True)
    (product_opener_dir / "DataQualityFood.pm").write_text(
        "package ProductOpener::DataQualityFood;\n1;\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("LEGACY_SOURCE_ROOT", raising=False)
    monkeypatch.setattr(legacy_source_module, "__file__", str(fake_module_path))

    assert resolve_legacy_source_root() == legacy_root
