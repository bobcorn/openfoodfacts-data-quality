from __future__ import annotations

from collections.abc import Callable

import pytest

from openfoodfacts_data_quality.checks.catalog import CheckCatalog
from openfoodfacts_data_quality.checks.sources import (
    default_dsl_check_pack_resources,
    default_python_check_pack_module_names,
)
from openfoodfacts_data_quality.contracts.checks import CheckDefinition, CheckSelection


def test_default_python_check_pack_module_names_discovers_pack_modules() -> None:
    assert default_python_check_pack_module_names() == (
        "openfoodfacts_data_quality.checks.packs.python.canada_checks",
        "openfoodfacts_data_quality.checks.packs.python.global_checks",
    )


def test_default_dsl_check_pack_resources_discovers_pack_files() -> None:
    assert tuple(resource.name for resource in default_dsl_check_pack_resources()) == (
        "canada_checks.yaml",
        "global_checks.yaml",
    )


def _selection_catalog(
    *,
    global_check_id: str,
    check_definition_factory: Callable[..., CheckDefinition],
    catalog_with_checks_factory: Callable[..., CheckCatalog],
) -> CheckCatalog:
    return catalog_with_checks_factory(
        check_definition_factory(global_check_id),
        check_definition_factory(
            "en:ca-runtime-check",
            parity_baseline="none",
            jurisdictions=("ca",),
        ),
    )


def test_catalog_selection_can_filter_by_parity_baseline_and_jurisdiction(
    check_definition_factory: Callable[..., CheckDefinition],
    catalog_with_checks_factory: Callable[..., CheckCatalog],
) -> None:
    catalog = _selection_catalog(
        global_check_id="en:legacy-global-check",
        check_definition_factory=check_definition_factory,
        catalog_with_checks_factory=catalog_with_checks_factory,
    )

    assert tuple(
        check.id
        for check in catalog.select_checks(
            selection=CheckSelection(parity_baselines=("legacy",))
        )
    ) == ("en:legacy-global-check",)
    assert tuple(
        check.id
        for check in catalog.select_checks(
            selection=CheckSelection(jurisdictions=("ca",))
        )
    ) == ("en:ca-runtime-check",)


def test_catalog_rejects_explicit_check_ids_outside_selection(
    check_definition_factory: Callable[..., CheckDefinition],
    catalog_with_checks_factory: Callable[..., CheckCatalog],
) -> None:
    catalog = _selection_catalog(
        global_check_id="en:global-check",
        check_definition_factory=check_definition_factory,
        catalog_with_checks_factory=catalog_with_checks_factory,
    )

    with pytest.raises(ValueError, match="parity baselines legacy"):
        catalog.select_checks(
            {"en:ca-runtime-check"},
            selection=CheckSelection(parity_baselines=("legacy",)),
        )


def test_check_definition_derives_legacy_identity_from_legacy_parity_baseline(
    check_definition_factory: Callable[..., CheckDefinition],
) -> None:
    legacy_check = check_definition_factory("en:legacy-global-check")
    runtime_only_check = check_definition_factory(
        "en:runtime-only-check",
        parity_baseline="none",
    )

    assert legacy_check.legacy_identity is not None
    assert legacy_check.legacy_identity.code_template == "en:legacy-global-check"
    assert runtime_only_check.legacy_identity is None
