from __future__ import annotations

from collections.abc import Mapping, Sequence

from off_data_quality._nutrition import first_non_estimated_as_sold_nutrients
from off_data_quality._scalars import as_number
from off_data_quality._source_products import (
    build_input_sets,
    build_source_product_classifier_fields,
    ingredient_tags_from_source_product,
)
from off_data_quality._structured_values import (
    is_string_object_mapping,
)
from off_data_quality.contracts.context import (
    CategoryPropsContext,
    FlagsContext,
    NutritionAsSoldContext,
    NutritionContext,
    ProductContext,
)
from off_data_quality.contracts.enrichment import (
    EnrichedCategoryPropsSnapshot,
    EnrichedFlagsSnapshot,
    EnrichedNutritionSnapshot,
    EnrichedProductSnapshot,
)
from off_data_quality.contracts.mapping_view import MappingViewModel
from off_data_quality.contracts.source_products import SourceProduct
from off_data_quality.contracts.structured import IngredientNode

SOURCE_PRODUCT_CONTEXT_PATHS: frozenset[str] = frozenset(
    (
        "product.code",
        "product.created_t",
        "product.product_name",
        "product.quantity",
        "product.product_quantity",
        "product.serving_size",
        "product.serving_quantity",
        "product.brands",
        "product.categories",
        "product.labels",
        "product.emb_codes",
        "product.ingredients_text",
        "product.ingredients_tags",
        "product.nutriscore_grade",
        "product.nutriscore_score",
        "product.categories_tags",
        "product.labels_tags",
        "product.countries_tags",
        "nutrition.input_sets",
        "nutrition.as_sold.energy_kcal",
        "nutrition.as_sold.fat",
        "nutrition.as_sold.saturated_fat",
        "nutrition.as_sold.trans_fat",
        "nutrition.as_sold.sugars",
        "nutrition.as_sold.fiber",
        "nutrition.as_sold.omega_3",
    )
)


def compact_mapping(
    values: Mapping[str, object] | MappingViewModel,
) -> dict[str, object]:
    """Remove None values while preserving false, zero, and empty collections."""
    mapping = values.as_mapping() if isinstance(values, MappingViewModel) else values
    return {key: value for key, value in mapping.items() if value is not None}


def ingredient_tags_from_ingredients(ingredients: Sequence[object]) -> list[str]:
    """Extract ingredient ids from a normalized ingredient list."""
    tags: list[str] = []
    for ingredient in ingredients:
        ingredient_id: object | None
        if isinstance(ingredient, IngredientNode):
            ingredient_id = ingredient.id
        elif is_string_object_mapping(ingredient):
            ingredient_id = ingredient.get("id")
        else:
            continue
        if isinstance(ingredient_id, str) and ingredient_id:
            tags.append(ingredient_id)
    return tags


def build_claim_nutrients(
    nutrition: MappingViewModel | dict[str, object],
) -> dict[str, float | None]:
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


def nutrient_value(nutrients: dict[str, object], nutrient_id: str) -> float | None:
    """Read one nutrient scalar from a prepared nutrient mapping."""
    nutrient = nutrients.get(nutrient_id)
    if not is_string_object_mapping(nutrient):
        return None
    return as_number(nutrient.get("value"))


def build_source_product_projection(row: SourceProduct) -> ProductContext:
    """Project one source product into the shared product subset."""
    classifier_fields = build_source_product_classifier_fields(row)
    return ProductContext.model_validate(
        compact_mapping(
            {
                "code": row.code,
                "product_name": row.product_name or None,
                "quantity": row.quantity or None,
                "product_quantity": as_number(row.product_quantity),
                "serving_size": row.serving_size or None,
                "serving_quantity": as_number(row.serving_quantity),
                "created_t": as_number(row.created_t),
                "brands": row.brands or None,
                "categories": row.categories or None,
                "labels": row.labels or None,
                "emb_codes": row.emb_codes or None,
                "ingredients_text": row.ingredients_text or None,
                "ingredients_tags": ingredient_tags_from_source_product(row),
                **classifier_fields,
            }
        )
    )


def build_source_nutrition_projection(row: SourceProduct) -> NutritionContext:
    """Project one source product into the shared nutrition context shape."""
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
    product_snapshot: EnrichedProductSnapshot,
) -> ProductContext:
    """Project one enriched snapshot into the shared product context shape."""
    ingredients = product_snapshot.ingredients
    return ProductContext.model_validate(
        compact_mapping(
            {
                "code": code,
                "lc": product_snapshot.lc,
                "lang": product_snapshot.lang,
                "created_t": product_snapshot.created_t,
                "packagings": product_snapshot.packagings,
                "product_name": product_snapshot.product_name,
                "quantity": product_snapshot.quantity,
                "product_quantity": product_snapshot.product_quantity,
                "serving_size": product_snapshot.serving_size,
                "serving_quantity": product_snapshot.serving_quantity,
                "brands": product_snapshot.brands,
                "categories": product_snapshot.categories,
                "labels": product_snapshot.labels,
                "emb_codes": product_snapshot.emb_codes,
                "ingredients_text": product_snapshot.ingredients_text,
                "ingredients": ingredients,
                "ingredients_tags": ingredient_tags_from_ingredients(ingredients),
                "ingredients_percent_analysis": product_snapshot.ingredients_percent_analysis,
                "ingredients_with_specified_percent_n": product_snapshot.ingredients_with_specified_percent_n,
                "ingredients_with_unspecified_percent_n": product_snapshot.ingredients_with_unspecified_percent_n,
                "ingredients_with_specified_percent_sum": product_snapshot.ingredients_with_specified_percent_sum,
                "ingredients_with_unspecified_percent_sum": product_snapshot.ingredients_with_unspecified_percent_sum,
                "nutriscore_grade": product_snapshot.nutriscore_grade,
                "nutriscore_grade_producer": product_snapshot.nutriscore_grade_producer,
                "nutriscore_score": product_snapshot.nutriscore_score,
                "categories_tags": product_snapshot.categories_tags,
                "labels_tags": product_snapshot.labels_tags,
                "countries_tags": product_snapshot.countries_tags,
                "food_groups_tags": product_snapshot.food_groups_tags,
            }
        )
    )


def build_enriched_flags_projection(
    flags_snapshot: EnrichedFlagsSnapshot,
) -> FlagsContext:
    """Project enriched snapshot flags into the shared context shape."""
    return FlagsContext(
        is_european_product=flags_snapshot.is_european_product,
        has_animal_origin_category=flags_snapshot.has_animal_origin_category,
        ignore_energy_calculated_error=flags_snapshot.ignore_energy_calculated_error,
    )


def build_enriched_category_props_projection(
    category_props_snapshot: EnrichedCategoryPropsSnapshot,
) -> CategoryPropsContext:
    """Project enriched snapshot category properties into the shared context shape."""
    return CategoryPropsContext.model_validate(
        compact_mapping(
            {
                "minimum_number_of_ingredients": category_props_snapshot.minimum_number_of_ingredients,
            }
        )
    )


def build_enriched_nutrition_projection(
    nutrition_snapshot: EnrichedNutritionSnapshot,
) -> NutritionContext:
    """Project enriched snapshot nutrition into the shared context shape."""
    return NutritionContext.model_validate(
        compact_mapping(
            {
                "input_sets": nutrition_snapshot.input_sets,
                "aggregated_set": nutrition_snapshot.aggregated_set,
                "as_sold": NutritionAsSoldContext.model_validate(
                    compact_mapping(build_claim_nutrients(nutrition_snapshot))
                ),
            }
        )
    )
