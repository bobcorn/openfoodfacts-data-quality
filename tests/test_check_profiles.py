from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from app.pipeline.profiles import load_check_profile

from openfoodfacts_data_quality.checks.catalog import (
    CheckCatalog,
    get_default_check_catalog,
    load_check_catalog,
)
from openfoodfacts_data_quality.checks.dsl.resources import dsl_check_pack_resources
from openfoodfacts_data_quality.contracts.checks import (
    LEGACY_PARITY_BASELINES,
    CheckDefinition,
)


def _global_dsl_pack_resource() -> Path:
    return next(
        Path(str(resource))
        for resource in dsl_check_pack_resources()
        if resource.name == "global_checks.yaml"
    )


def test_load_check_profile_uses_default_all_profile(tmp_path: Path) -> None:
    config_path = tmp_path / "check-profiles.toml"
    config_path.write_text(
        """
default_profile = "all"

[profiles.all]
description = "Runs every legacy-backed check."
mode = "all"
""".strip(),
        encoding="utf-8",
    )

    profile = load_check_profile(config_path)

    assert profile.name == "all"
    assert profile.check_input_surface == "enriched_products"
    assert profile.parity_baselines == LEGACY_PARITY_BASELINES
    assert len(profile.checks) > 1
    assert profile.check_ids == tuple(check.id for check in profile.checks)


def test_load_check_profile_returns_registry_order_for_included_checks(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "check-profiles.toml"
    config_path.write_text(
        """
default_profile = "focus"

[profiles.focus]
description = "Runs a focused workset."
mode = "include"
check_ids = [
  "en:serving-quantity-over-product-quantity",
  "en:quantity-not-recognized",
]
""".strip(),
        encoding="utf-8",
    )

    profile = load_check_profile(config_path)

    assert profile.check_ids == (
        "en:quantity-not-recognized",
        "en:serving-quantity-over-product-quantity",
    )


def test_load_check_profile_filters_all_checks_to_raw_products_surface(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "check-profiles.toml"
    config_path.write_text(
        """
default_profile = "raw_products"

[profiles.raw_products]
description = "Runs checks supported on the Open Food Facts public database schema."
mode = "all"
check_input_surface = "raw_products"
""".strip(),
        encoding="utf-8",
    )

    profile = load_check_profile(config_path)

    assert profile.check_input_surface == "raw_products"
    assert "en:created-missing" in profile.check_ids
    assert "en:serving-quantity-over-product-quantity" in profile.check_ids
    assert "en:main-language-code-missing" not in profile.check_ids
    assert (
        "en:ingredients-count-lower-than-expected-for-the-category"
        not in profile.check_ids
    )


def test_load_check_profile_rejects_include_checks_unsupported_on_surface(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "check-profiles.toml"
    config_path.write_text(
        """
default_profile = "raw_products"

[profiles.raw_products]
description = "Runs a focused raw-products workset."
mode = "include"
check_input_surface = "raw_products"
check_ids = ["en:main-language-code-missing"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError, match="references checks outside input surface raw_products"
    ):
        load_check_profile(config_path)


def test_load_check_profile_rejects_unknown_check_ids(tmp_path: Path) -> None:
    config_path = tmp_path / "check-profiles.toml"
    config_path.write_text(
        """
default_profile = "focus"

[profiles.focus]
description = "Runs a focused workset."
mode = "include"
check_ids = ["en:not-a-real-check"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unknown checks"):
        load_check_profile(config_path)


def test_load_check_profile_excludes_runtime_only_checks_by_default(
    tmp_path: Path,
    check_definition_factory: Callable[..., CheckDefinition],
    catalog_with_checks_factory: Callable[..., CheckCatalog],
) -> None:
    catalog = catalog_with_checks_factory(
        check_definition_factory("en:legacy-check"),
        check_definition_factory(
            "en:ca-runtime-check",
            parity_baseline="none",
            jurisdictions=("ca",),
        ),
    )
    config_path = tmp_path / "check-profiles.toml"
    config_path.write_text(
        """
default_profile = "all"

[profiles.all]
description = "Runs every legacy-backed check."
mode = "all"
check_input_surface = "raw_products"
""".strip(),
        encoding="utf-8",
    )

    profile = load_check_profile(config_path, catalog=catalog)

    assert profile.check_ids == ("en:legacy-check",)


def test_load_check_catalog_select_evaluators_filters_to_active_check_ids() -> None:
    evaluators = load_check_catalog(_global_dsl_pack_resource()).select_evaluators(
        {
            "en:quantity-not-recognized",
            "en:serving-quantity-over-product-quantity",
        }
    )

    assert tuple(evaluators) == (
        "en:quantity-not-recognized",
        "en:serving-quantity-over-product-quantity",
    )


def test_catalog_derives_supported_input_surfaces() -> None:
    checks_by_id = get_default_check_catalog().checks_by_id
    assert checks_by_id[
        "en:serving-quantity-over-product-quantity"
    ].supported_input_surfaces == (
        "raw_products",
        "enriched_products",
    )
    assert checks_by_id["en:created-missing"].supported_input_surfaces == (
        "raw_products",
        "enriched_products",
    )
    assert checks_by_id["en:main-language-code-missing"].supported_input_surfaces == (
        "enriched_products",
    )
    assert checks_by_id[
        "en:ingredients-count-lower-than-expected-for-the-category"
    ].supported_input_surfaces == ("enriched_products",)
