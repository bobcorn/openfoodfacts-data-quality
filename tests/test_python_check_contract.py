from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from openfoodfacts_data_quality.checks.catalog import (
    CheckCatalog,
    get_default_check_catalog,
    load_check_catalog,
)
from openfoodfacts_data_quality.checks.registry import CheckBinding, check_bindings

if TYPE_CHECKING:
    from collections.abc import Iterator

_OMEGA_3_CHECK_ID = (
    "en:source-of-omega-3-label-claim-but-ala-or-sum-of-epa-and-dha-below-limitation"
)
_CANADA_TRANS_FAT_FREE_CHECK_ID = (
    "ca:trans-fat-free-claim-but-nutrition-does-not-meet-conditions"
)


def test_default_catalog_keeps_declared_context_paths_for_omega_3_check() -> None:
    check_definition = get_default_check_catalog().check_by_id(_OMEGA_3_CHECK_ID)
    binding = next(
        binding
        for binding in _bindings_for_default_python_packs()
        if binding.id == _OMEGA_3_CHECK_ID
    )

    expected_paths = (
        "product.labels_tags",
        "nutrition.input_sets",
    )
    assert check_definition.required_context_paths == expected_paths
    assert binding.required_context_paths == expected_paths


def test_default_catalog_keeps_declared_context_paths_for_canada_trans_fat_free_check() -> (
    None
):
    check_definition = get_default_check_catalog().check_by_id(
        _CANADA_TRANS_FAT_FREE_CHECK_ID
    )
    binding = next(
        binding
        for binding in _bindings_for_default_python_packs()
        if binding.id == _CANADA_TRANS_FAT_FREE_CHECK_ID
    )

    expected_paths = (
        "product.labels_tags",
        "nutrition.as_sold.energy_kcal",
        "nutrition.as_sold.saturated_fat",
        "nutrition.as_sold.trans_fat",
    )
    assert check_definition.required_context_paths == expected_paths
    assert binding.required_context_paths == expected_paths


def test_catalog_uses_python_declared_context_paths_as_the_contract(
    tmp_path: Path,
) -> None:
    with _loaded_temp_check_catalog(
        tmp_path,
        module_name="temp_declared_context_path_checks",
        source="""
from openfoodfacts_data_quality.checks.registry import check
from openfoodfacts_data_quality.contracts.checks import CheckEmission, CheckPackMetadata
from openfoodfacts_data_quality.contracts.context import CheckContext

CHECK_PACK_METADATA = CheckPackMetadata(
    parity_baseline="legacy",
    jurisdictions=("global",),
)

def helper(value: object) -> object:
    return value

@check("en:contract-test", requires=("product.product_name",))
def en_contract_test(context: CheckContext) -> list[CheckEmission]:
    helper(context.nutrition)
    if context.product.labels_tags:
        return []
    return [CheckEmission(severity="warning")]
""",
    ) as catalog:
        check_definition = catalog.check_by_id("en:contract-test")

    assert check_definition.required_context_paths == ("product.product_name",)


def test_catalog_rejects_unknown_python_context_path(tmp_path: Path) -> None:
    with pytest.raises(
        ValueError,
        match="Check en:contract-test declares unknown context paths: product.unknown",
    ):
        with _loaded_temp_check_catalog(
            tmp_path,
            module_name="temp_unknown_context_path_checks",
            source="""
from openfoodfacts_data_quality.checks.registry import check
from openfoodfacts_data_quality.contracts.checks import CheckEmission, CheckPackMetadata
from openfoodfacts_data_quality.contracts.context import CheckContext

CHECK_PACK_METADATA = CheckPackMetadata(
    parity_baseline="legacy",
    jurisdictions=("global",),
)

@check("en:contract-test", requires=("product.unknown",))
def en_contract_test(context: CheckContext) -> list[CheckEmission]:
    return [CheckEmission(severity="warning")]
""",
        ):
            pass


def test_catalog_rejects_python_check_pack_without_pack_metadata(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ValueError,
        match=(
            "Python check pack temp_missing_pack_metadata_checks "
            "must declare CHECK_PACK_METADATA."
        ),
    ):
        with _loaded_temp_check_catalog(
            tmp_path,
            module_name="temp_missing_pack_metadata_checks",
            source="""
from openfoodfacts_data_quality.checks.registry import check
from openfoodfacts_data_quality.contracts.checks import CheckEmission
from openfoodfacts_data_quality.contracts.context import CheckContext

@check("en:contract-test", requires=("product.product_name",))
def en_contract_test(context: CheckContext) -> list[CheckEmission]:
    if context.product.product_name:
        return []
    return [CheckEmission(severity="warning")]
""",
        ):
            pass


def test_catalog_rejects_duplicate_canonical_legacy_identity_mappings(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ValueError,
        match=(
            "Duplicate legacy identity mappings: "
            "en:ingredients-\\$\\{\\*\\}-photo-selected: "
            "en:ingredients-\\$\\{display_lc\\}-photo-selected, "
            "en:ingredients-\\$\\{lang_code\\}-photo-selected"
        ),
    ):
        with _loaded_temp_check_catalog(
            tmp_path,
            module_name="temp_duplicate_legacy_identity_checks",
            source="""
from openfoodfacts_data_quality.checks.registry import check
from openfoodfacts_data_quality.contracts.checks import CheckEmission, CheckPackMetadata
from openfoodfacts_data_quality.contracts.context import CheckContext

CHECK_PACK_METADATA = CheckPackMetadata(
    parity_baseline="legacy",
    jurisdictions=("global",),
)

@check(
    "en:ingredients-${lang_code}-photo-selected",
    requires=("product.product_name",),
)
def en_first_contract_test(context: CheckContext) -> list[CheckEmission]:
    if context.product.product_name:
        return []
    return [CheckEmission(severity="warning")]


@check(
    "en:ingredients-${display_lc}-photo-selected",
    requires=("product.product_name",),
)
def en_second_contract_test(context: CheckContext) -> list[CheckEmission]:
    if context.product.product_name:
        return []
    return [CheckEmission(severity="warning")]
""",
        ):
            pass


@contextmanager
def _loaded_temp_check_catalog(
    tmp_path: Path,
    *,
    module_name: str,
    source: str,
) -> Iterator[CheckCatalog]:
    module_path = tmp_path / f"{module_name}.py"
    module_path.write_text(source.strip(), encoding="utf-8")
    sys.path.insert(0, str(tmp_path))
    importlib.invalidate_caches()
    try:
        yield load_check_catalog(
            definitions_paths=(),
            python_module_names=(module_name,),
        )
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop(module_name, None)
        importlib.invalidate_caches()


def _bindings_for_default_python_packs() -> tuple[CheckBinding, ...]:
    module_names = (
        "openfoodfacts_data_quality.checks.packs.python.canada_checks",
        "openfoodfacts_data_quality.checks.packs.python.global_checks",
    )
    return tuple(
        binding
        for module_name in module_names
        for binding in check_bindings(import_module(module_name))
    )
