from __future__ import annotations

import pytest

from openfoodfacts_data_quality.context.paths import path_spec_for
from openfoodfacts_data_quality.contracts.context import (
    NormalizedContext,
    ProductContext,
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
    assert countries_tags_spec.supported_input_surfaces == (
        "raw_products",
        "enriched_products",
    )
    assert omega_3_spec.type == "number"
    assert energy_kcal_spec.type == "number"
    assert ingredients_spec.dsl_allowed is False
    assert ingredients_spec.supported_input_surfaces == ("enriched_products",)


def test_normalized_context_rejects_mismatched_product_code() -> None:
    with pytest.raises(ValueError, match="product.code"):
        NormalizedContext(
            code="123",
            product=ProductContext(code="456"),
        )
