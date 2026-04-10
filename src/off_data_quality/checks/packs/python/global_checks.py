"""Packaged global Python quality checks."""

from __future__ import annotations

from typing import TYPE_CHECKING

from off_data_quality._nutrition import first_non_estimated_as_sold_nutrients
from off_data_quality._scalars import as_number
from off_data_quality._structured_values import is_object_list
from off_data_quality.checks._check_helpers import (
    energy_mismatch_for_unit,
    has_food_group_level,
    ingredient_claim_has_unknowns,
    iter_nutrient_family_sets,
    nutrient_value,
    present,
    single_emission,
    sugars_starch_fiber_for_set,
)
from off_data_quality.checks._registry import check
from off_data_quality.contracts.checks import CheckEmission, CheckPackMetadata

if TYPE_CHECKING:
    from off_data_quality.contracts.context import CheckContext

CHECK_PACK_METADATA = CheckPackMetadata(
    parity_baseline="legacy",
    jurisdictions=("global",),
)


@check(
    "en:food-groups-${level}-known",
    requires=("product.food_groups_tags",),
)
def en_food_groups_var_level_known(
    context: CheckContext,
) -> list[CheckEmission]:
    """Emit one finding for each known food group level."""
    food_groups_tags = context.product.food_groups_tags
    emissions: list[CheckEmission] = []
    for level in range(1, 4):
        if has_food_group_level(food_groups_tags, level):
            emissions.append(
                CheckEmission(
                    raw_code=f"en:food-groups-{level}-known",
                    severity="info",
                )
            )
    return emissions


@check(
    "en:food-groups-${level}-unknown",
    requires=("product.food_groups_tags",),
)
def en_food_groups_var_level_unknown(
    context: CheckContext,
) -> list[CheckEmission]:
    """Emit one finding for each missing food group level."""
    food_groups_tags = context.product.food_groups_tags
    emissions: list[CheckEmission] = []
    for level in range(1, 4):
        if not has_food_group_level(food_groups_tags, level):
            emissions.append(
                CheckEmission(
                    raw_code=f"en:food-groups-{level}-unknown",
                    severity="info",
                )
            )
    return emissions


@check(
    "en:serving-quantity-over-product-quantity",
    requires=("product.serving_quantity", "product.product_quantity"),
)
def en_serving_quantity_over_product_quantity(
    context: CheckContext,
) -> list[CheckEmission]:
    """Flag servings larger than the whole product quantity."""
    serving_quantity = as_number(context.product.serving_quantity)
    product_quantity = as_number(context.product.product_quantity)
    if (
        serving_quantity is not None
        and product_quantity is not None
        and serving_quantity > product_quantity
    ):
        return single_emission("warning")
    return []


@check(
    "en:nutriscore-grade-producer-mismatch-nok",
    requires=(
        "product.nutriscore_grade",
        "product.nutriscore_grade_producer",
    ),
)
def en_nutriscore_grade_producer_mismatch_nok(
    context: CheckContext,
) -> list[CheckEmission]:
    """Flag mismatches between producer and computed Nutri-Score grades."""
    producer_grade = context.product.nutriscore_grade_producer
    computed_grade = context.product.nutriscore_grade
    if (
        present(producer_grade)
        and present(computed_grade)
        and producer_grade != computed_grade
    ):
        return single_emission("warning")
    return []


@check(
    "en:serving-quantity-less-than-product-quantity-divided-by-1000",
    requires=("product.serving_quantity", "product.product_quantity"),
)
def en_serving_quantity_less_than_product_quantity_divided_by_1000(
    context: CheckContext,
) -> list[CheckEmission]:
    """Flag implausibly tiny serving quantities."""
    serving_quantity = as_number(context.product.serving_quantity)
    product_quantity = as_number(context.product.product_quantity)
    if (
        serving_quantity is not None
        and product_quantity is not None
        and serving_quantity < product_quantity / 1000
    ):
        return single_emission("warning")
    return []


@check(
    "en:ingredients-count-lower-than-expected-for-the-category",
    requires=(
        "category_props.minimum_number_of_ingredients",
        "product.ingredients",
    ),
)
def en_ingredients_count_lower_than_expected_for_the_category(
    context: CheckContext,
) -> list[CheckEmission]:
    """Flag products with too few ingredients for their category."""
    minimum = as_number(context.category_props.minimum_number_of_ingredients)
    ingredient_count = len(context.product.ingredients or [])
    if minimum is None:
        return []
    if ingredient_count > 0 and ingredient_count < minimum:
        return single_emission("error")
    return []


@check(
    "en:${set_id}-energy-value-in-${unit}-does-not-match-value-computed-from-other-nutrients",
    requires=(
        "flags.ignore_energy_calculated_error",
        "nutrition.input_sets",
        "nutrition.aggregated_set",
    ),
)
def en_var_set_id_energy_value_in_var_unit_does_not_match_value_computed_from_other_nutrients(
    context: CheckContext,
) -> list[CheckEmission]:
    """Emit one finding per nutrient set and energy unit with a computed-value mismatch."""
    if context.flags.ignore_energy_calculated_error is True:
        return []

    emissions: list[CheckEmission] = []
    for set_id, nutrients, severity in iter_nutrient_family_sets(context):
        for unit in ("kj", "kcal"):
            if energy_mismatch_for_unit(nutrients, unit):
                emissions.append(
                    CheckEmission(
                        raw_code=(
                            f"en:{set_id}-energy-value-in-{unit}"
                            "-does-not-match-value-computed-from-other-nutrients"
                        ),
                        severity=severity,
                    )
                )
    return emissions


@check(
    "en:${set_id}-sugars-plus-starch-plus-fiber-greater-than-carbohydrates-total",
    requires=("nutrition.input_sets", "nutrition.aggregated_set"),
)
def en_var_set_id_sugars_plus_starch_plus_fiber_greater_than_carbohydrates_total(
    context: CheckContext,
) -> list[CheckEmission]:
    """Emit one finding per nutrient set whose parts exceed total carbohydrates."""
    emissions: list[CheckEmission] = []
    for set_id, nutrients, severity in iter_nutrient_family_sets(context):
        if sugars_starch_fiber_for_set(nutrients):
            emissions.append(
                CheckEmission(
                    raw_code=(
                        f"en:{set_id}-sugars-plus-starch-plus-fiber-"
                        "greater-than-carbohydrates-total"
                    ),
                    severity=severity,
                )
            )
    return emissions


@check(
    "en:source-of-omega-3-label-claim-but-ala-or-sum-of-epa-and-dha-below-limitation",
    requires=("product.labels_tags", "nutrition.input_sets"),
)
def en_source_of_omega_3_label_claim_but_ala_or_sum_of_epa_and_dha_below_limitation(
    context: CheckContext,
) -> list[CheckEmission]:
    """Flag omega-3 claims that do not meet the nutrient threshold."""
    label_tags = set(context.product.labels_tags)
    if "en:source-of-omega-3" not in label_tags:
        return []

    nutrients = first_non_estimated_as_sold_nutrients(context.nutrition)
    if not nutrients:
        return []

    alpha_linolenic_acid = nutrient_value(nutrients, "alpha-linolenic-acid")
    if alpha_linolenic_acid is not None and alpha_linolenic_acid < 0.3:
        return single_emission("warning")

    eicosapentaenoic_acid = nutrient_value(nutrients, "eicosapentaenoic-acid")
    docosahexaenoic_acid = nutrient_value(nutrients, "docosahexaenoic-acid")
    if (
        eicosapentaenoic_acid is not None
        and docosahexaenoic_acid is not None
        and (eicosapentaenoic_acid + docosahexaenoic_acid) < 0.04
    ):
        return single_emission("warning")

    return []


@check(
    "en:vegan-label-but-could-not-confirm-for-all-ingredients",
    requires=("product.labels_tags", "product.ingredients"),
)
def en_vegan_label_but_could_not_confirm_for_all_ingredients(
    context: CheckContext,
) -> list[CheckEmission]:
    """Flag vegan labels whose ingredients cannot be confidently confirmed as vegan."""
    label_tags = set(context.product.labels_tags)
    raw_ingredients = context.product.ingredients
    if "en:vegan" not in label_tags or not is_object_list(raw_ingredients):
        return []
    if not raw_ingredients:
        return []

    if ingredient_claim_has_unknowns(raw_ingredients, "vegan"):
        return single_emission("warning")
    return []
