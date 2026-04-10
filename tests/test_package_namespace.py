from __future__ import annotations

import importlib

import pytest

import off_data_quality


def test_root_namespace_exports_only_consumer_namespaces() -> None:
    assert off_data_quality.__all__ == ["checks", "snapshots"]


def test_supported_facades_live_under_off_data_quality() -> None:
    module = importlib.import_module("off_data_quality.catalog")
    execution_module = importlib.import_module("off_data_quality.execution")

    assert hasattr(module, "CheckCatalog")
    assert not hasattr(execution_module, "iter_with_progress")


@pytest.mark.parametrize(
    ("module_name"),
    [
        "off_data_quality.checks.catalog",
        "off_data_quality.checks.check_helpers",
        "off_data_quality.checks.engine",
        "off_data_quality.checks.sources",
        "off_data_quality.checks.legacy",
        "off_data_quality.checks.registry",
        "off_data_quality.context.builder",
        "off_data_quality.context.paths",
        "off_data_quality.context.projection",
        "off_data_quality.context.providers",
        "off_data_quality.nutrition",
        "off_data_quality.progress",
        "off_data_quality.scalars",
        "off_data_quality.source_product_preparation",
        "off_data_quality.structured_values",
    ],
)
def test_internal_module_paths_are_not_public(module_name: str) -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module_name)


def test_openfoodfacts_data_quality_namespace_is_gone() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("openfoodfacts_data_quality")
