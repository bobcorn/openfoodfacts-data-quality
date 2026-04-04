from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

import pytest
from app.legacy_backend.input_projection import (
    build_legacy_backend_input_products,
)
from app.reference.models import (
    ReferenceResult,
    enriched_snapshots_from_reference_results,
)
from app.run.context_builders import check_context_builder_for

from openfoodfacts_data_quality.context.builder import (
    build_enriched_contexts,
    build_raw_contexts,
)
from openfoodfacts_data_quality.contracts.checks import CheckInputSurface
from openfoodfacts_data_quality.contracts.raw import (
    RawProductRow,
    validate_raw_product_row,
)

ReferenceResultFactory = Callable[..., ReferenceResult]


def _raw_product_row(**overrides: Any) -> RawProductRow:
    row = {
        "code": "123",
        "created_t": "123",
        "product_name": "Example",
        "quantity": "500 g",
        "product_quantity": "500",
        "serving_size": "50 g",
        "serving_quantity": "50",
        "brands": "Brand",
        "categories": "Supplements",
        "labels": "No gluten",
        "emb_codes": "FR 01.001",
        "ingredients_text": "Sugar, salt",
        "ingredients_tags": "en:sugar,en:salt",
        "nutriscore_grade": "A",
        "nutriscore_score": "-2",
        "labels_tags": "en:vegan,en:nutriscore-grade-a",
        "categories_tags": "en:supplements,en:dietary-supplements",
        "countries_tags": "en:france,en:canada",
        "energy-kcal_100g": "123",
        "fat_100g": "3.5",
        "saturated-fat_100g": "0.2",
        "trans-fat_100g": "0.1",
        "sugars_100g": "1.5",
        "fiber_100g": "6.5",
        "omega-3-fat_100g": "0.4",
        "no_nutrition_data": "",
    }
    row.update(overrides)
    return validate_raw_product_row(row)


def _mismatched_row_and_backend_result(
    reference_result_factory: ReferenceResultFactory,
) -> tuple[RawProductRow, ReferenceResult]:
    # The raw surface owns the canonical row contract even in parity side tests.
    row = _raw_product_row(product_name="Raw name")
    reference_result = reference_result_factory(
        code="123",
        enriched_snapshot={
            "product": {
                "code": "123",
                "product_name": "Enriched name",
                "lang": "fr",
            }
        },
    )
    return row, reference_result


def test_build_legacy_backend_input_products_projects_backend_input_shape() -> None:
    rows = [_raw_product_row()]

    backend_input_product = build_legacy_backend_input_products(rows)[0]

    assert backend_input_product.code == "123"
    assert "lang" not in backend_input_product.projected_input
    assert backend_input_product.projected_input["created_t"] == 123.0
    assert backend_input_product.projected_input["product_quantity"] == 500.0
    assert backend_input_product.projected_input["serving_quantity"] == 50.0
    assert backend_input_product.projected_input["labels_tags"] == [
        "en:vegan",
        "en:nutriscore-grade-a",
    ]
    assert backend_input_product.projected_input["countries_tags"] == [
        "en:france",
        "en:canada",
    ]
    assert "lc" not in backend_input_product.projected_input
    assert "product_type" not in backend_input_product.projected_input
    assert "nutriscore_grade_producer" not in backend_input_product.projected_input
    assert backend_input_product.projected_input["ingredients"] == [
        {"id": "en:sugar"},
        {"id": "en:salt"},
    ]
    nutrition = cast(
        dict[str, object],
        backend_input_product.projected_input["nutrition"],
    )
    input_sets = cast(list[dict[str, object]], nutrition["input_sets"])
    nutrients = cast(dict[str, dict[str, float]], input_sets[0]["nutrients"])
    assert nutrients["energy-kcal"]["value"] == 123.0
    assert nutrients["fat"]["value"] == 3.5


def test_build_raw_contexts_uses_raw_product_rows() -> None:
    rows = [_raw_product_row(no_nutrition_data=None)]

    context = build_raw_contexts(rows)[0]

    assert context.code == "123"
    assert context.product.lang is None
    assert context.product.created_t == 123.0
    assert context.product.product_name == "Example"
    assert context.product.ingredients_tags == ["en:sugar", "en:salt"]
    assert context.product.labels_tags == ["en:vegan", "en:nutriscore-grade-a"]
    assert context.product.lc is None
    assert context.product.ingredients is None
    assert context.flags.as_mapping() == {}
    assert context.category_props.as_mapping() == {}
    nutrients = context.nutrition["input_sets"][0]["nutrients"]
    assert nutrients["fat"]["value"] == 3.5
    assert context.nutrition.as_sold.as_mapping() == {
        "energy_kcal": 123.0,
        "fat": 3.5,
        "saturated_fat": 0.2,
        "trans_fat": 0.1,
        "sugars": 1.5,
        "fiber": 6.5,
        "omega_3": 0.4,
    }


def test_build_raw_contexts_normalizes_public_source_rows() -> None:
    context = build_raw_contexts(
        [
            {
                "code": "123",
                "created_t": 123,
                "product_name": [{"lang": "main", "text": "Example"}],
                "quantity": "500 g",
                "product_quantity": "500",
                "serving_size": "50 g",
                "serving_quantity": "50",
                "ingredients_text": [{"lang": "main", "text": "Sugar, salt"}],
                "ingredients_tags": ["en:sugar", "en:salt"],
                "labels_tags": ["en:vegan", "en:nutriscore-grade-a"],
                "categories_tags": ["en:supplements", "en:dietary-supplements"],
                "countries_tags": ["en:france", "en:canada"],
                "nutriments": [
                    {"name": "energy-kcal", "100g": 123.0},
                    {"name": "fat", "100g": 3.5},
                ],
            }
        ]
    )[0]

    assert context.code == "123"
    assert context.product.product_name == "Example"
    assert context.product.ingredients_tags == ["en:sugar", "en:salt"]
    assert context.product.labels_tags == ["en:vegan", "en:nutriscore-grade-a"]
    assert context.nutrition.as_sold.as_mapping() == {
        "energy_kcal": 123.0,
        "fat": 3.5,
    }


def test_build_enriched_contexts_uses_backend_enriched_snapshot(
    reference_result_factory: ReferenceResultFactory,
) -> None:
    reference_result = reference_result_factory(
        code="123",
        enriched_snapshot={
            "product": {
                "code": "123",
                "product_name": "Prepared name",
                "quantity": "Prepared quantity",
                "lang": "fr",
                "created_t": "123",
                "product_quantity": "500.0",
                "ingredients": [{"id": "en:sugar"}],
                "nutriscore_grade_producer": "a",
                "labels_tags": ["en:vegetarian", "en:nutriscore-grade-a"],
                "countries_tags": ["en:canada"],
                "food_groups_tags": ["en:plant-based-foods"],
            },
            "flags": {
                "is_european_product": True,
                "has_animal_origin_category": True,
                "ignore_energy_calculated_error": True,
            },
            "category_props": {
                "minimum_number_of_ingredients": "4",
            },
            "nutrition": {
                "input_sets": [
                    {
                        "source": "packaging",
                        "preparation": "as_sold",
                        "per": "100g",
                        "nutrients": {
                            "energy-kcal": {"value": 120},
                            "saturated-fat": {"value": 0.3},
                            "trans-fat": {"value": 0.05},
                            "fiber": {"value": 5},
                            "omega-3-fat": {"value": 0.4},
                        },
                    }
                ],
                "aggregated_set": {"nutrients": {"energy-kcal": {"value": 120}}},
            },
        },
    )

    context = build_enriched_contexts(
        enriched_snapshots_from_reference_results([reference_result])
    )[0]

    assert context.product.product_name == "Prepared name"
    assert context.product.quantity == "Prepared quantity"
    assert context.product.code == "123"
    assert context.product.lang == "fr"
    assert context.product.created_t == 123.0
    assert context.product.labels_tags == [
        "en:vegetarian",
        "en:nutriscore-grade-a",
    ]
    assert context.product.ingredients_tags == ["en:sugar"]
    assert context.product.nutriscore_grade_producer == "a"
    assert context.product.food_groups_tags == ["en:plant-based-foods"]
    assert context.flags.as_mapping() == {
        "is_european_product": True,
        "has_animal_origin_category": True,
        "ignore_energy_calculated_error": True,
    }
    assert context.category_props.minimum_number_of_ingredients == 4.0
    assert context.nutrition.as_sold.as_mapping() == {
        "energy_kcal": 120.0,
        "saturated_fat": 0.3,
        "trans_fat": 0.05,
        "fiber": 5.0,
        "omega_3": 0.4,
    }
    assert context.nutrition.aggregated_set is not None
    assert context.nutrition.aggregated_set.as_mapping() == {
        "nutrients": {"energy-kcal": {"value": 120}}
    }


def test_build_enriched_contexts_rejects_snapshot_code_mismatch(
    reference_result_factory: ReferenceResultFactory,
) -> None:
    with pytest.raises(
        ValueError,
        match="Reference result code must match enriched_snapshot.product.code",
    ):
        reference_result_factory(
            code="123",
            enriched_snapshot={"product": {"code": "456"}},
        )


@pytest.mark.parametrize(
    ("input_surface", "expected_product_name", "expected_lang"),
    [
        ("raw_products", "Raw name", None),
        ("enriched_products", "Enriched name", "fr"),
    ],
)
def test_check_context_builder_uses_selected_input_surface(
    input_surface: CheckInputSurface,
    expected_product_name: str,
    expected_lang: str | None,
    reference_result_factory: ReferenceResultFactory,
) -> None:
    builder = check_context_builder_for(input_surface)
    row, reference_result = _mismatched_row_and_backend_result(reference_result_factory)

    context = builder.build_contexts(
        rows=[row],
        enriched_snapshots=enriched_snapshots_from_reference_results(
            [reference_result]
        ),
    )[0]

    assert builder.input_surface == input_surface
    assert builder.requires_enriched_snapshots is (input_surface == "enriched_products")
    assert context.product.product_name == expected_product_name
    assert context.product.lang == expected_lang
