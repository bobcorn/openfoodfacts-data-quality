from __future__ import annotations

from collections.abc import Callable

from off_data_quality.catalog import get_default_check_catalog
from off_data_quality.checks import CheckEvaluator
from off_data_quality.checks.packs.python.canada_checks import (
    ca_trans_fat_free_claim_but_nutrition_does_not_meet_conditions,
)
from off_data_quality.contracts.context import CheckContext

ContextFactory = Callable[..., CheckContext]

_CATALOG = get_default_check_catalog()


def _evaluator_for(check_id: str) -> CheckEvaluator:
    return _CATALOG.select_evaluators({check_id})[check_id]


def test_canada_source_of_fibre_dsl_check_warns_below_threshold(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={"labels_tags": ["en:source-of-fibre"]},
        nutrition={"as_sold": {"fiber": 1.9}},
    )

    emissions = _evaluator_for("ca:source-of-fibre-claim-but-fibre-below-threshold")(
        context
    )

    assert [emission.severity for emission in emissions] == ["warning"]


def test_canada_source_of_omega_3_dsl_check_warns_below_threshold(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={"labels_tags": ["en:source-of-omega-3-polyunsaturates"]},
        nutrition={"as_sold": {"omega_3": 0.2}},
    )

    emissions = _evaluator_for(
        "ca:source-of-omega-3-polyunsaturates-claim-but-omega-3-below-threshold"
    )(context)

    assert [emission.severity for emission in emissions] == ["warning"]


def test_canada_trans_fat_free_python_check_warns_for_trans_fat_threshold_breach(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={"labels_tags": ["en:trans-fat-free"]},
        nutrition={
            "as_sold": {
                "energy_kcal": 100,
                "saturated_fat": 0.1,
                "trans_fat": 0.2,
            }
        },
    )

    assert (
        ca_trans_fat_free_claim_but_nutrition_does_not_meet_conditions(context)[
            0
        ].severity
        == "warning"
    )


def test_canada_trans_fat_free_python_check_warns_for_energy_share_breach(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={"labels_tags": ["en:trans-fat-free"]},
        nutrition={
            "as_sold": {
                "energy_kcal": 40,
                "saturated_fat": 0.6,
                "trans_fat": 0.1,
            }
        },
    )

    assert (
        ca_trans_fat_free_claim_but_nutrition_does_not_meet_conditions(context)[
            0
        ].severity
        == "warning"
    )


def test_canada_trans_fat_free_python_check_passes_when_conditions_are_met(
    context_factory: ContextFactory,
) -> None:
    context = context_factory(
        product={"labels_tags": ["en:trans-fat-free"]},
        nutrition={
            "as_sold": {
                "energy_kcal": 100,
                "saturated_fat": 0.4,
                "trans_fat": 0.1,
            }
        },
    )

    assert ca_trans_fat_free_claim_but_nutrition_does_not_meet_conditions(context) == []
