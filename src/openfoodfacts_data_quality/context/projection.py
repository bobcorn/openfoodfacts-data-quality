from __future__ import annotations

from collections.abc import Mapping

from openfoodfacts_data_quality.contracts.context import (
    CategoryPropsContext,
    FlagsContext,
    NutritionAsSoldContext,
    NutritionContext,
    ProductContext,
)
from openfoodfacts_data_quality.nutrition import first_non_estimated_as_sold_nutrients
from openfoodfacts_data_quality.raw_products import (
    build_input_sets,
    build_raw_classifier_fields,
    ingredient_tags_from_raw_row,
)
from openfoodfacts_data_quality.scalars import as_number
from openfoodfacts_data_quality.structured_values import (
    StringObjectMapping,
    is_string_object_mapping,
    object_list_or_empty,
)


def compact_mapping(values: Mapping[str, object]) -> dict[str, object]:
    """Remove None values while preserving false, zero, and empty collections."""
    return {key: value for key, value in values.items() if value is not None}


def list_value(value: object) -> list[object]:
    """Normalize an arbitrary list-like field into a list."""
    return object_list_or_empty(value)


def list_string_value(value: object) -> list[str]:
    """Normalize a list-like field into a string list."""
    list_items = object_list_or_empty(value)
    if not list_items:
        return []
    normalized: list[str] = []
    for item in list_items:
        text = str(item)
        if text:
            normalized.append(text)
    return normalized


def ingredient_tags_from_ingredients(ingredients: list[object]) -> list[str]:
    """Extract ingredient ids from a normalized ingredient list."""
    tags: list[str] = []
    for ingredient in ingredients:
        if not is_string_object_mapping(ingredient):
            continue
        ingredient_id = ingredient.get("id")
        if isinstance(ingredient_id, str) and ingredient_id:
            tags.append(ingredient_id)
    return tags


def build_claim_nutrients(nutrition: Mapping[str, object]) -> dict[str, float | None]:
    """Materialize claim-facing scalar nutrients from the first as-sold set."""
    nutrients = first_non_estimated_as_sold_nutrients(nutrition)
    return {
        "energy_kcal": nutrient_value(nutrients, "energy-kcal"),
        "fat": nutrient_value(nutrients, "fat"),
        "saturated_fat": nutrient_value(nutrients, "saturated-fat"),
        "trans_fat": nutrient_value(nutrients, "trans-fat"),
        "sugars": nutrient_value(nutrients, "sugars"),
        "fiber": nutrient_value(nutrients, "fiber"),
        "omega_3": nutrient_value(nutrients, "omega-3-fat"),
    }


def nutrient_value(nutrients: Mapping[str, object], nutrient_id: str) -> float | None:
    """Read one nutrient scalar from a prepared nutrient mapping."""
    nutrient = nutrients.get(nutrient_id)
    if not is_string_object_mapping(nutrient):
        return None
    return as_number(nutrient.get("value"))


def build_raw_product_projection(row: StringObjectMapping) -> ProductContext:
    """Project one raw product row into the shared product subset."""
    classifier_fields = build_raw_classifier_fields(row)
    return ProductContext.model_validate(
        compact_mapping(
            {
                "code": str(row["code"]),
                "product_name": row.get("product_name") or None,
                "quantity": row.get("quantity") or None,
                "product_quantity": as_number(row.get("product_quantity")),
                "serving_size": row.get("serving_size") or None,
                "serving_quantity": as_number(row.get("serving_quantity")),
                "created_t": as_number(row.get("created_t")),
                "brands": row.get("brands") or None,
                "categories": row.get("categories") or None,
                "labels": row.get("labels") or None,
                "emb_codes": row.get("emb_codes") or None,
                "ingredients_text": row.get("ingredients_text") or None,
                "ingredients_tags": ingredient_tags_from_raw_row(row),
                **classifier_fields,
            }
        )
    )


def build_raw_nutrition_projection(row: StringObjectMapping) -> NutritionContext:
    """Project one raw product row into the shared nutrition context shape."""
    input_sets = build_input_sets(row)
    return NutritionContext.model_validate(
        {
            "input_sets": input_sets,
            "as_sold": NutritionAsSoldContext.model_validate(
                compact_mapping(build_claim_nutrients({"input_sets": input_sets}))
            ),
        }
    )


def build_enriched_product_projection(
    *,
    code: str,
    product_snapshot: StringObjectMapping,
) -> ProductContext:
    """Project one enriched snapshot into the shared product context shape."""
    ingredients = list_value(product_snapshot.get("ingredients"))
    return ProductContext.model_validate(
        compact_mapping(
            {
                "code": code,
                "lc": product_snapshot.get("lc"),
                "lang": product_snapshot.get("lang"),
                "created_t": as_number(product_snapshot.get("created_t")),
                "packagings": list_value(product_snapshot.get("packagings")),
                "product_name": product_snapshot.get("product_name"),
                "quantity": product_snapshot.get("quantity"),
                "product_quantity": as_number(product_snapshot.get("product_quantity")),
                "serving_size": product_snapshot.get("serving_size"),
                "serving_quantity": as_number(product_snapshot.get("serving_quantity")),
                "brands": product_snapshot.get("brands"),
                "categories": product_snapshot.get("categories"),
                "labels": product_snapshot.get("labels"),
                "emb_codes": product_snapshot.get("emb_codes"),
                "ingredients_text": product_snapshot.get("ingredients_text"),
                "ingredients": ingredients,
                "ingredients_tags": ingredient_tags_from_ingredients(ingredients),
                "ingredients_percent_analysis": as_number(
                    product_snapshot.get("ingredients_percent_analysis")
                ),
                "ingredients_with_specified_percent_n": as_number(
                    product_snapshot.get("ingredients_with_specified_percent_n")
                ),
                "ingredients_with_unspecified_percent_n": as_number(
                    product_snapshot.get("ingredients_with_unspecified_percent_n")
                ),
                "ingredients_with_specified_percent_sum": as_number(
                    product_snapshot.get("ingredients_with_specified_percent_sum")
                ),
                "ingredients_with_unspecified_percent_sum": as_number(
                    product_snapshot.get("ingredients_with_unspecified_percent_sum")
                ),
                "nutriscore_grade": product_snapshot.get("nutriscore_grade"),
                "nutriscore_grade_producer": product_snapshot.get(
                    "nutriscore_grade_producer"
                ),
                "nutriscore_score": as_number(product_snapshot.get("nutriscore_score")),
                "categories_tags": list_string_value(
                    product_snapshot.get("categories_tags")
                ),
                "labels_tags": list_string_value(product_snapshot.get("labels_tags")),
                "countries_tags": list_string_value(
                    product_snapshot.get("countries_tags")
                ),
                "food_groups_tags": list_string_value(
                    product_snapshot.get("food_groups_tags")
                ),
            }
        )
    )


def build_enriched_flags_projection(
    flags_snapshot: StringObjectMapping,
) -> FlagsContext:
    """Project enriched snapshot flags into the shared context shape."""
    return FlagsContext(
        is_european_product=bool(flags_snapshot.get("is_european_product")),
        has_animal_origin_category=bool(
            flags_snapshot.get("has_animal_origin_category")
        ),
        ignore_energy_calculated_error=bool(
            flags_snapshot.get("ignore_energy_calculated_error")
        ),
    )


def build_enriched_category_props_projection(
    category_props_snapshot: StringObjectMapping,
) -> CategoryPropsContext:
    """Project enriched snapshot category properties into the shared context shape."""
    return CategoryPropsContext.model_validate(
        compact_mapping(
            {
                "minimum_number_of_ingredients": as_number(
                    category_props_snapshot.get("minimum_number_of_ingredients")
                ),
            }
        )
    )


def build_enriched_nutrition_projection(
    nutrition_snapshot: StringObjectMapping,
) -> NutritionContext:
    """Project enriched snapshot nutrition into the shared context shape."""
    aggregated_set = nutrition_snapshot.get("aggregated_set")
    return NutritionContext.model_validate(
        compact_mapping(
            {
                "input_sets": list_value(nutrition_snapshot.get("input_sets")),
                "aggregated_set": (
                    aggregated_set if is_string_object_mapping(aggregated_set) else None
                ),
                "as_sold": NutritionAsSoldContext.model_validate(
                    compact_mapping(build_claim_nutrients(nutrition_snapshot))
                ),
            }
        )
    )
