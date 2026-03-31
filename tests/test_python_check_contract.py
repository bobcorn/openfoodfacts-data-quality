from __future__ import annotations

import importlib
import sys
from importlib import import_module
from pathlib import Path

import pytest

from openfoodfacts_data_quality.checks.catalog import (
    get_default_check_catalog,
    load_check_catalog,
)
from openfoodfacts_data_quality.checks.context_dependencies import (
    infer_check_context_dependencies,
)
from openfoodfacts_data_quality.checks.registry import CheckBinding, check_bindings

_OMEGA_3_CHECK_ID = (
    "en:source-of-omega-3-label-claim-but-ala-or-sum-of-epa-and-dha-below-limitation"
)
_CANADA_TRANS_FAT_FREE_CHECK_ID = (
    "ca:trans-fat-free-claim-but-nutrition-does-not-meet-conditions"
)


def test_default_catalog_keeps_declared_contract_for_omega_3_check() -> None:
    check_definition = get_default_check_catalog().check_by_id(_OMEGA_3_CHECK_ID)
    binding = next(
        binding
        for binding in infer_bindings_for_default_catalog()
        if binding.id == _OMEGA_3_CHECK_ID
    )

    assert check_definition.required_context_paths == (
        "product.labels_tags",
        "nutrition.input_sets",
    )
    assert tuple(
        dependency.path for dependency in infer_check_context_dependencies(binding)
    ) == (
        "product.labels_tags",
        "nutrition.input_sets",
    )


def test_catalog_rejects_python_check_with_missing_direct_and_helper_dependencies(
    tmp_path: Path,
) -> None:
    module_name = "temp_contract_checks"
    module_path = tmp_path / "temp_contract_checks.py"
    module_path.write_text(
        """
from openfoodfacts_data_quality.checks.context_dependencies import depends_on_context_paths
from openfoodfacts_data_quality.checks.registry import check
from openfoodfacts_data_quality.contracts.checks import CheckEmission, CheckPackMetadata
from openfoodfacts_data_quality.contracts.context import NormalizedContext

CHECK_PACK_METADATA = CheckPackMetadata(
    parity_baseline="legacy",
    jurisdictions=("global",),
)

@depends_on_context_paths("nutrition.input_sets")
def first_input_set(nutrition: object) -> object:
    return nutrition

@check("en:contract-test", requires=("product.product_name",))
def en_contract_test(context: NormalizedContext) -> list[CheckEmission]:
    if context.product.labels_tags:
        return []
    first_input_set(context.nutrition)
    return [CheckEmission(severity="warning")]
""".strip(),
        encoding="utf-8",
    )
    sys.path.insert(0, str(tmp_path))
    importlib.invalidate_caches()

    try:
        with pytest.raises(
            ValueError,
            match=(
                "Python check en:contract-test is missing declared context paths: "
                "product.labels_tags \\(direct access to context.product.labels_tags\\), "
                "nutrition.input_sets \\(helper dependency via first_input_set\\)"
            ),
        ):
            load_check_catalog(
                definitions_paths=(),
                python_module_names=(module_name,),
            )
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop(module_name, None)
        importlib.invalidate_caches()


def test_catalog_rejects_python_check_pack_without_pack_metadata(
    tmp_path: Path,
) -> None:
    module_name = "temp_missing_pack_metadata_checks"
    module_path = tmp_path / "temp_missing_pack_metadata_checks.py"
    module_path.write_text(
        """
from openfoodfacts_data_quality.checks.registry import check
from openfoodfacts_data_quality.contracts.checks import CheckEmission
from openfoodfacts_data_quality.contracts.context import NormalizedContext

@check("en:contract-test", requires=("product.product_name",))
def en_contract_test(context: NormalizedContext) -> list[CheckEmission]:
    if context.product.product_name:
        return []
    return [CheckEmission(severity="warning")]
""".strip(),
        encoding="utf-8",
    )
    sys.path.insert(0, str(tmp_path))
    importlib.invalidate_caches()

    try:
        with pytest.raises(
            ValueError,
            match=(
                "Python check pack temp_missing_pack_metadata_checks "
                "must declare CHECK_PACK_METADATA."
            ),
        ):
            load_check_catalog(
                definitions_paths=(),
                python_module_names=(module_name,),
            )
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop(module_name, None)
        importlib.invalidate_caches()


def test_catalog_accepts_python_check_legacy_code_template_override(
    tmp_path: Path,
) -> None:
    module_name = "temp_legacy_identity_checks"
    module_path = tmp_path / "temp_legacy_identity_checks.py"
    module_path.write_text(
        """
from openfoodfacts_data_quality.checks.registry import check
from openfoodfacts_data_quality.contracts.checks import CheckEmission, CheckPackMetadata
from openfoodfacts_data_quality.contracts.context import NormalizedContext

CHECK_PACK_METADATA = CheckPackMetadata(
    parity_baseline="legacy",
    jurisdictions=("global",),
)

@check(
    "en:contract-test",
    requires=("product.product_name",),
    legacy_code_template="en:legacy-contract-test",
)
def en_contract_test(context: NormalizedContext) -> list[CheckEmission]:
    if context.product.product_name:
        return []
    return [CheckEmission(severity="warning")]
""".strip(),
        encoding="utf-8",
    )
    sys.path.insert(0, str(tmp_path))
    importlib.invalidate_caches()

    try:
        catalog = load_check_catalog(
            definitions_paths=(),
            python_module_names=(module_name,),
        )
        legacy_identity = catalog.check_by_id("en:contract-test").legacy_identity
        assert legacy_identity is not None
        assert legacy_identity.code_template == "en:legacy-contract-test"
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop(module_name, None)
        importlib.invalidate_caches()


def test_catalog_rejects_duplicate_canonical_legacy_identity_mappings(
    tmp_path: Path,
) -> None:
    module_name = "temp_duplicate_legacy_identity_checks"
    module_path = tmp_path / "temp_duplicate_legacy_identity_checks.py"
    module_path.write_text(
        """
from openfoodfacts_data_quality.checks.registry import check
from openfoodfacts_data_quality.contracts.checks import CheckEmission, CheckPackMetadata
from openfoodfacts_data_quality.contracts.context import NormalizedContext

CHECK_PACK_METADATA = CheckPackMetadata(
    parity_baseline="legacy",
    jurisdictions=("global",),
)

@check(
    "en:first-contract-test",
    requires=("product.product_name",),
    legacy_code_template="en:ingredients-${lang_code}-photo-selected",
)
def en_first_contract_test(context: NormalizedContext) -> list[CheckEmission]:
    if context.product.product_name:
        return []
    return [CheckEmission(severity="warning")]


@check(
    "en:second-contract-test",
    requires=("product.product_name",),
    legacy_code_template="en:ingredients-${display_lc}-photo-selected",
)
def en_second_contract_test(context: NormalizedContext) -> list[CheckEmission]:
    if context.product.product_name:
        return []
    return [CheckEmission(severity="warning")]
""".strip(),
        encoding="utf-8",
    )
    sys.path.insert(0, str(tmp_path))
    importlib.invalidate_caches()

    try:
        with pytest.raises(
            ValueError,
            match=(
                "Duplicate legacy identity mappings: "
                "en:ingredients-\\$\\{\\*\\}-photo-selected: "
                "en:first-contract-test, en:second-contract-test"
            ),
        ):
            load_check_catalog(
                definitions_paths=(),
                python_module_names=(module_name,),
            )
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop(module_name, None)
        importlib.invalidate_caches()


def test_default_catalog_keeps_declared_contract_for_canada_trans_fat_free_check() -> (
    None
):
    check_definition = get_default_check_catalog().check_by_id(
        _CANADA_TRANS_FAT_FREE_CHECK_ID
    )
    binding = next(
        binding
        for binding in infer_bindings_for_default_catalog()
        if binding.id == _CANADA_TRANS_FAT_FREE_CHECK_ID
    )

    assert check_definition.required_context_paths == (
        "product.labels_tags",
        "nutrition.as_sold.energy_kcal",
        "nutrition.as_sold.saturated_fat",
        "nutrition.as_sold.trans_fat",
    )
    assert tuple(
        dependency.path for dependency in infer_check_context_dependencies(binding)
    ) == (
        "product.labels_tags",
        "nutrition.as_sold.energy_kcal",
        "nutrition.as_sold.saturated_fat",
        "nutrition.as_sold.trans_fat",
    )


def infer_bindings_for_default_catalog() -> tuple[CheckBinding, ...]:
    module_names = (
        "openfoodfacts_data_quality.checks.packs.python.canada_checks",
        "openfoodfacts_data_quality.checks.packs.python.global_checks",
    )
    return tuple(
        binding
        for module_name in module_names
        for binding in check_bindings(import_module(module_name))
    )
