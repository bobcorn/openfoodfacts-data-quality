from __future__ import annotations

import pytest

from openfoodfacts_data_quality.context.builder import build_source_product_contexts
from openfoodfacts_data_quality.context.paths import (
    MISSING,
    PATH_SPECS,
    path_spec_for,
    resolve_path,
)
from openfoodfacts_data_quality.context.providers import (
    ENRICHED_SNAPSHOTS_PROVIDER,
    SOURCE_PRODUCTS_PROVIDER,
    context_availability_for_provider,
)
from openfoodfacts_data_quality.contracts.context import (
    CheckContext,
    ProductContext,
)
from openfoodfacts_data_quality.contracts.source_products import (
    validate_source_product,
)


def test_context_path_specs_cover_stable_projected_fields() -> None:
    brands_spec = path_spec_for("product.brands")
    countries_tags_spec = path_spec_for("product.countries_tags")
    score_spec = path_spec_for("product.nutriscore_score")
    ingredients_spec = path_spec_for("product.ingredients")
    omega_3_spec = path_spec_for("nutrition.as_sold.omega_3")
    energy_kcal_spec = path_spec_for("nutrition.as_sold.energy_kcal")

    assert brands_spec is not None
    assert countries_tags_spec is not None
    assert score_spec is not None
    assert ingredients_spec is not None
    assert omega_3_spec is not None
    assert energy_kcal_spec is not None
    assert countries_tags_spec.type == "array"
    assert countries_tags_spec.dsl_allowed is True
    assert omega_3_spec.type == "number"
    assert energy_kcal_spec.type == "number"
    assert ingredients_spec.dsl_allowed is False


def test_context_provider_contracts_own_path_availability() -> None:
    source_products_availability = context_availability_for_provider("source_products")
    enriched_snapshots_availability = context_availability_for_provider(
        "enriched_snapshots"
    )
    all_context_paths = frozenset(spec.path for spec in PATH_SPECS)

    assert source_products_availability is SOURCE_PRODUCTS_PROVIDER.availability
    assert enriched_snapshots_availability is ENRICHED_SNAPSHOTS_PROVIDER.availability
    assert enriched_snapshots_availability.available_context_paths == all_context_paths
    assert (
        "product.countries_tags" in source_products_availability.available_context_paths
    )
    assert (
        "product.ingredients"
        not in source_products_availability.available_context_paths
    )
    assert (
        "product.ingredients" in enriched_snapshots_availability.available_context_paths
    )


def test_source_product_provider_availability_matches_projection_output() -> None:
    context = build_source_product_contexts(
        [
            validate_source_product(
                {
                    "code": "123",
                    "created_t": 1,
                    "product_name": "Example",
                    "quantity": "100 g",
                    "product_quantity": 100,
                    "serving_size": "50 g",
                    "serving_quantity": 50,
                    "brands": "Brand",
                    "categories": "Category",
                    "labels": "Label",
                    "emb_codes": "EMB",
                    "ingredients_text": "Sugar",
                    "ingredients_tags": "en:sugar",
                    "nutriscore_grade": "A",
                    "nutriscore_score": 1,
                    "categories_tags": "en:snacks",
                    "labels_tags": "en:organic",
                    "countries_tags": "en:canada",
                    "energy-kcal_100g": 10,
                    "fat_100g": 1,
                    "saturated-fat_100g": 0.2,
                    "trans-fat_100g": 0,
                    "sugars_100g": 3,
                    "fiber_100g": 4,
                    "omega-3-fat_100g": 0.1,
                }
            )
        ]
    )[0]
    context_mapping = context.as_mapping()

    for context_path in SOURCE_PRODUCTS_PROVIDER.availability.available_context_paths:
        assert resolve_path(context_mapping, context_path) is not MISSING


def test_check_context_rejects_mismatched_product_code() -> None:
    with pytest.raises(ValueError, match="product.code"):
        CheckContext(
            code="123",
            product=ProductContext(code="456"),
        )
