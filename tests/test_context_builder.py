from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from migration.legacy_backend.input_payloads import build_legacy_backend_input_payloads
from migration.reference.models import (
    ReferenceResult,
    reference_check_contexts_from_reference_results,
)
from migration.run.context_builders import check_context_builder_for
from migration.source.models import ProductDocument
from migration.source.product_documents import validate_product_document

from off_data_quality.checks import prepare
from off_data_quality.context import build_source_product_contexts
from off_data_quality.contracts.source_products import (
    SourceProduct,
    validate_source_product,
)

ReferenceResultFactory = Callable[..., ReferenceResult]


def _source_product(**overrides: Any) -> SourceProduct:
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
    return validate_source_product(row)


def _product_document(**overrides: Any) -> ProductDocument:
    document = {
        "code": "123",
        "created_t": "123",
        "product_name": "Example",
        "lang": "fr",
        "countries_tags": ["en:france", "en:canada"],
        "nutriments": {"energy-kcal_100g": 123.0, "fat_100g": 3.5},
        "empty_array": [],
        "false_flag": False,
    }
    document.update(overrides)
    return validate_product_document(document)


def _mismatched_row_and_backend_result(
    reference_result_factory: ReferenceResultFactory,
) -> tuple[SourceProduct, ReferenceResult]:
    # The source products provider owns the canonical row contract even in parity side tests.
    row = _source_product(product_name="Source name")
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


def test_build_legacy_backend_input_payloads_serializes_full_product_document() -> None:
    product_documents = [_product_document()]

    backend_input_payload = build_legacy_backend_input_payloads(product_documents)[0]

    assert backend_input_payload["code"] == "123"
    assert backend_input_payload["lang"] == "fr"
    assert backend_input_payload["countries_tags"] == [
        "en:france",
        "en:canada",
    ]
    assert backend_input_payload["empty_array"] == []
    assert backend_input_payload["false_flag"] is False


def test_build_source_product_contexts_uses_source_products() -> None:
    rows = [_source_product(no_nutrition_data=None)]

    context = build_source_product_contexts(rows)[0]

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


def test_build_source_product_contexts_accepts_prepared_source_products() -> None:
    prepared_rows = prepare(
        [
            {
                "code": "123",
                "created_t": 123,
                "product_name": "Example",
                "quantity": "500 g",
                "product_quantity": "500",
                "serving_size": "50 g",
                "serving_quantity": "50",
                "ingredients_text": "Sugar, salt",
                "ingredients_tags": ["en:sugar", "en:salt"],
                "labels_tags": ["en:vegan", "en:nutriscore-grade-a"],
                "categories_tags": ["en:supplements", "en:dietary-supplements"],
                "countries_tags": ["en:france", "en:canada"],
                "energy-kcal_100g": 123.0,
                "fat_100g": 3.5,
            }
        ]
    )
    context = build_source_product_contexts(prepared_rows)[0]

    assert context.code == "123"
    assert context.product.product_name == "Example"
    assert context.product.ingredients_tags == ["en:sugar", "en:salt"]
    assert context.product.labels_tags == ["en:vegan", "en:nutriscore-grade-a"]
    assert context.nutrition.as_sold.as_mapping() == {
        "energy_kcal": 123.0,
        "fat": 3.5,
    }


def test_build_enriched_snapshot_contexts_uses_backend_enriched_snapshot(
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

    context = reference_check_contexts_from_reference_results([reference_result])[0]

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


def test_build_enriched_snapshot_contexts_rejects_snapshot_code_mismatch(
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


def test_check_context_builder_uses_enriched_snapshot_provider(
    reference_result_factory: ReferenceResultFactory,
) -> None:
    builder = check_context_builder_for("enriched_snapshots")
    _, reference_result = _mismatched_row_and_backend_result(reference_result_factory)

    context = builder.build_contexts(
        reference_check_contexts=reference_check_contexts_from_reference_results(
            [reference_result]
        ),
    )[0]

    assert builder.context_provider == "enriched_snapshots"
    assert builder.requires_reference_check_contexts is True
    assert context.product.product_name == "Enriched name"
    assert context.product.lang == "fr"


def test_check_context_builder_rejects_source_products_provider() -> None:
    with pytest.raises(
        ValueError,
        match="strict parity only supports the 'enriched_snapshots' context provider",
    ):
        check_context_builder_for("source_products")
