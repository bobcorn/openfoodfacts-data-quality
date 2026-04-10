from __future__ import annotations

from collections.abc import Callable

import pytest

from off_data_quality.catalog import CheckCatalog
from off_data_quality.contracts.checks import CheckDefinition, CheckSelection
from off_data_quality.metadata import (
    packaged_dsl_check_pack_resource_path,
    packaged_dsl_check_pack_resources,
    packaged_module_path,
    packaged_python_check_pack_module_names,
    packaged_runtime_fingerprint,
)


def test_packaged_python_check_pack_module_names_discovers_pack_modules() -> None:
    assert packaged_python_check_pack_module_names() == (
        "off_data_quality.checks.packs.python.canada_checks",
        "off_data_quality.checks.packs.python.global_checks",
    )


def test_packaged_dsl_check_pack_resources_discovers_pack_files() -> None:
    assert tuple(resource.name for resource in packaged_dsl_check_pack_resources()) == (
        "canada_checks.yaml",
        "global_checks.yaml",
    )


def test_packaged_metadata_exposes_wheel_relative_paths_and_runtime_fingerprint() -> (
    None
):
    dsl_resources = packaged_dsl_check_pack_resources()

    assert packaged_module_path(
        "off_data_quality.checks.packs.python.global_checks"
    ) == ("off_data_quality/checks/packs/python/global_checks.py")
    assert packaged_dsl_check_pack_resource_path(dsl_resources[0]).startswith(
        "off_data_quality/checks/packs/dsl/"
    )
    assert len(packaged_runtime_fingerprint()) == 64


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
