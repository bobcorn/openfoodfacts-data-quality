from __future__ import annotations

from collections.abc import Callable
from importlib.resources.abc import Traversable
from pathlib import Path

import pytest
from migration.run.profiles import load_check_profile

from off_data_quality.catalog import (
    CheckCatalog,
    get_default_check_catalog,
    load_check_catalog,
)
from off_data_quality.checks.dsl.resources import dsl_check_pack_resources
from off_data_quality.context import context_availability_for_provider
from off_data_quality.contracts.capabilities import resolve_check_capabilities
from off_data_quality.contracts.checks import (
    LEGACY_PARITY_BASELINES,
    CheckDefinition,
)


def _global_dsl_pack_resource() -> Traversable:
    return next(
        resource
        for resource in dsl_check_pack_resources()
        if resource.name == "global_checks.yaml"
    )


def test_load_check_profile_uses_default_all_profile(tmp_path: Path) -> None:
    config_path = tmp_path / "check-profiles.toml"
    config_path.write_text(
        """
default_profile = "all"

[profiles.all]
description = "Runs every check with a legacy baseline."
mode = "all"
""".strip(),
        encoding="utf-8",
    )

    profile = load_check_profile(config_path)

    assert profile.name == "all"
    assert profile.check_context_provider == "enriched_snapshots"
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


def test_load_check_profile_filters_all_checks_to_source_products_provider(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "check-profiles.toml"
    config_path.write_text(
        """
default_profile = "source_products"

[profiles.source_products]
description = "Runs checks supported on the Open Food Facts source product schema."
mode = "all"
check_context_provider = "source_products"
""".strip(),
        encoding="utf-8",
    )

    profile = load_check_profile(config_path)

    assert profile.check_context_provider == "source_products"
    assert "en:created-missing" in profile.check_ids
    assert "en:serving-quantity-over-product-quantity" in profile.check_ids
    assert "en:main-language-code-missing" not in profile.check_ids
    assert (
        "en:ingredients-count-lower-than-expected-for-the-category"
        not in profile.check_ids
    )


def test_load_check_profile_rejects_include_checks_unsupported_on_provider(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "check-profiles.toml"
    config_path.write_text(
        """
default_profile = "source_products"

[profiles.source_products]
description = "Runs a focused source product workset."
mode = "include"
check_context_provider = "source_products"
check_ids = ["en:main-language-code-missing"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError, match="references checks outside context provider source_products"
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
description = "Runs every check with a legacy baseline."
mode = "all"
check_context_provider = "source_products"
""".strip(),
        encoding="utf-8",
    )

    profile = load_check_profile(config_path, catalog=catalog)

    assert profile.check_ids == ("en:legacy-check",)


def test_load_check_profile_rejects_removed_migration_metadata_filters(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "check-profiles.toml"
    config_path.write_text(
        """
default_profile = "dsl_focus"

[profiles.dsl_focus]
description = "Attempts to use removed migration metadata filters."
mode = "all"
check_context_provider = "source_products"
migration_target_impls = ["dsl"]
migration_risks = ["low"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="uses removed migration planning fields",
    ):
        load_check_profile(config_path)


def test_shipped_full_profile_includes_runtime_only_checks() -> None:
    profile = load_check_profile(
        Path(__file__).resolve().parents[1] / "config" / "check-profiles.toml",
        profile_name="full",
    )

    assert profile.parity_baselines == ("legacy", "none")
    assert "ca:trans-fat-free-claim-but-nutrition-does-not-meet-conditions" in (
        profile.check_ids
    )


def test_shipped_legacy_profile_excludes_runtime_only_checks() -> None:
    profile = load_check_profile(
        Path(__file__).resolve().parents[1] / "config" / "check-profiles.toml",
        profile_name="legacy",
    )

    assert profile.check_context_provider == "enriched_snapshots"
    assert profile.parity_baselines == ("legacy",)
    assert "ca:trans-fat-free-claim-but-nutrition-does-not-meet-conditions" not in (
        profile.check_ids
    )


def test_shipped_source_products_profile_excludes_runtime_only_checks() -> None:
    profile = load_check_profile(
        Path(__file__).resolve().parents[1] / "config" / "check-profiles.toml",
        profile_name="source_products",
    )

    assert profile.check_context_provider == "source_products"
    assert profile.parity_baselines == ("legacy",)
    assert "ca:source-of-fibre-claim-but-fibre-below-threshold" not in profile.check_ids


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


def test_context_availability_filters_default_catalog_checks() -> None:
    checks_by_id = get_default_check_catalog().checks_by_id
    source_report = resolve_check_capabilities(
        checks_by_id.values(),
        context_availability_for_provider("source_products"),
    )
    enriched_report = resolve_check_capabilities(
        checks_by_id.values(),
        context_availability_for_provider("enriched_snapshots"),
    )

    source_check_ids = {
        capability.check_id for capability in source_report.runnable_capabilities
    }
    enriched_check_ids = {
        capability.check_id for capability in enriched_report.runnable_capabilities
    }

    assert "en:serving-quantity-over-product-quantity" in source_check_ids
    assert "en:created-missing" in source_check_ids
    assert "en:main-language-code-missing" not in source_check_ids
    assert (
        "en:ingredients-count-lower-than-expected-for-the-category"
        not in source_check_ids
    )
    assert "en:main-language-code-missing" in enriched_check_ids
    assert "en:ingredients-count-lower-than-expected-for-the-category" in (
        enriched_check_ids
    )
