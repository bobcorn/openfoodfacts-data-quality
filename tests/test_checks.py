from __future__ import annotations

from collections.abc import Callable

from off_data_quality.checks.packs.python.global_checks import (
    en_food_groups_var_level_known,
    en_food_groups_var_level_unknown,
    en_ingredients_count_lower_than_expected_for_the_category,
    en_source_of_omega_3_label_claim_but_ala_or_sum_of_epa_and_dha_below_limitation,
    en_var_set_id_energy_value_in_var_unit_does_not_match_value_computed_from_other_nutrients,
    en_var_set_id_sugars_plus_starch_plus_fiber_greater_than_carbohydrates_total,
    en_vegan_label_but_could_not_confirm_for_all_ingredients,
)
from off_data_quality.contracts.context import CheckContext

ContextFactory = Callable[..., CheckContext]


def test_ingredients_count_lower_than_expected_for_category_uses_category_props(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={
            "ingredients": [{"id": "en:sugar"}, {"id": "en:salt"}],
        },
        category_props={
            "minimum_number_of_ingredients": 3.0,
        },
    )

    assert (
        en_ingredients_count_lower_than_expected_for_the_category(context)[0].severity
        == "error"
    )


def test_food_groups_known_returns_info_when_at_least_one_level_exists(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={
            "food_groups_tags": ["en:plant-based-foods"],
        },
    )

    emissions = en_food_groups_var_level_known(context)

    assert [(emission.raw_code, emission.severity) for emission in emissions] == [
        ("en:food-groups-1-known", "info")
    ]


def test_food_groups_unknown_returns_info_when_a_level_is_missing(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={
            "food_groups_tags": ["en:plant-based-foods"],
        },
    )

    emissions = en_food_groups_var_level_unknown(context)

    assert [(emission.raw_code, emission.severity) for emission in emissions] == [
        ("en:food-groups-2-unknown", "info"),
        ("en:food-groups-3-unknown", "info"),
    ]


def test_energy_kcal_mismatch_from_other_nutrients_detects_error(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        nutrition={
            "input_sets": [
                {
                    "source": "packaging",
                    "preparation": "as_sold",
                    "nutrients": {
                        "energy-kcal": {
                            "value": 100,
                            "value_computed": 20,
                        }
                    },
                }
            ]
        }
    )

    emissions = en_var_set_id_energy_value_in_var_unit_does_not_match_value_computed_from_other_nutrients(
        context
    )

    assert [(emission.raw_code, emission.severity) for emission in emissions] == [
        (
            "en:nutrition-packaging-as-sold-unknown-per-energy-value-in-kcal-does-not-match-value-computed-from-other-nutrients",
            "error",
        )
    ]


def test_sugars_starch_fiber_exceed_total_carbohydrates_detects_error(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        nutrition={
            "input_sets": [
                {
                    "source": "packaging",
                    "preparation": "as_sold",
                    "nutrients": {
                        "carbohydrates-total": {"value": 10},
                        "sugars": {"value": 6},
                        "starch": {"value": 3},
                        "fiber": {"value": 2},
                    },
                }
            ]
        }
    )

    emissions = (
        en_var_set_id_sugars_plus_starch_plus_fiber_greater_than_carbohydrates_total(
            context
        )
    )

    assert [(emission.raw_code, emission.severity) for emission in emissions] == [
        (
            "en:nutrition-packaging-as-sold-unknown-per-sugars-plus-starch-plus-fiber-greater-than-carbohydrates-total",
            "error",
        )
    ]


def test_vegan_label_warns_when_vegan_claim_cannot_be_confirmed(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={
            "labels_tags": ["en:vegan"],
            "ingredients": [{"id": "en:sugar"}],
        }
    )

    assert en_vegan_label_but_could_not_confirm_for_all_ingredients(context)[
        0
    ].severity == ("warning")


def test_vegan_label_ignores_products_with_maybe_only_ingredients(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={
            "labels_tags": ["en:vegan"],
            "ingredients": [{"id": "en:flavouring", "vegan": "maybe"}],
        }
    )

    assert en_vegan_label_but_could_not_confirm_for_all_ingredients(context) == []


def test_vegan_label_ignores_products_with_fully_vegan_ingredients(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={
            "labels_tags": ["en:vegan"],
            "ingredients": [{"id": "en:sugar", "vegan": "yes"}],
        }
    )

    assert en_vegan_label_but_could_not_confirm_for_all_ingredients(context) == []


def test_vegan_label_ignores_products_with_non_vegan_ingredients(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={
            "labels_tags": ["en:vegan"],
            "ingredients": [{"id": "en:beef", "vegan": "no"}],
        }
    )

    assert en_vegan_label_but_could_not_confirm_for_all_ingredients(context) == []


def test_vegan_label_warns_for_nested_unknown_ingredients(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={
            "labels_tags": ["en:vegan"],
            "ingredients": [
                {
                    "id": "en:seasoning",
                    "vegan": "yes",
                    "ingredients": [{"id": "en:flavouring"}],
                }
            ],
        }
    )

    assert en_vegan_label_but_could_not_confirm_for_all_ingredients(context)[
        0
    ].severity == ("warning")


def test_vegan_label_ignores_products_without_ingredients(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={
            "labels_tags": ["en:vegan"],
            "ingredients": [],
        }
    )

    assert en_vegan_label_but_could_not_confirm_for_all_ingredients(context) == []


def test_omega3_claim_requires_matching_label(context_factory: ContextFactory) -> None:
    context = context_factory(
        product={
            "labels_tags": [],
        },
        nutrition={
            "input_sets": [
                {
                    "source": "packaging",
                    "preparation": "as_sold",
                    "nutrients": {
                        "proteins": {"value": 5},
                    },
                }
            ]
        },
    )

    assert (
        en_source_of_omega_3_label_claim_but_ala_or_sum_of_epa_and_dha_below_limitation(
            context
        )
        == []
    )


def test_omega3_claim_warns_when_ala_is_below_threshold(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={
            "labels_tags": ["en:source-of-omega-3"],
        },
        nutrition={
            "input_sets": [
                {
                    "source": "packaging",
                    "preparation": "as_sold",
                    "nutrients": {
                        "alpha-linolenic-acid": {"value": 0.2},
                    },
                }
            ]
        },
    )

    assert (
        en_source_of_omega_3_label_claim_but_ala_or_sum_of_epa_and_dha_below_limitation(
            context
        )[0].severity
        == "warning"
    )
