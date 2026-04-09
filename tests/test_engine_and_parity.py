from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

import pytest
from app.parity.comparator import evaluate_parity
from app.run.accumulator import RunResultAccumulator

from openfoodfacts_data_quality.checks.catalog import (
    CheckCatalog,
    get_default_check_catalog,
)
from openfoodfacts_data_quality.checks.engine import (
    CheckRunOptions,
    run_checks_with_evaluators,
)
from openfoodfacts_data_quality.contracts.checks import CheckDefinition
from openfoodfacts_data_quality.contracts.context import CheckContext
from openfoodfacts_data_quality.contracts.findings import Finding
from openfoodfacts_data_quality.contracts.observations import ObservedFinding
from openfoodfacts_data_quality.contracts.run import RunMetadata, RunResult

_QUANTITY_CHECK_ID = "en:quantity-not-recognized"

ContextFactory = Callable[..., CheckContext]
ObservedFindingFactory = Callable[..., ObservedFinding]


def _run_checks(
    contexts: list[CheckContext],
    *,
    active_check_ids: set[str] | None = None,
) -> list[Finding]:
    catalog = get_default_check_catalog()
    return run_checks_with_evaluators(
        contexts,
        check_evaluators=catalog.select_evaluators(active_check_ids),
        options=CheckRunOptions(
            catalog=catalog,
            log_loaded=False,
            log_progress=False,
        ),
    )


def _quantity_finding(
    observed_finding_factory: ObservedFindingFactory, *, product_id: str, side: str
) -> ObservedFinding:
    return observed_finding_factory(
        check_id=_QUANTITY_CHECK_ID,
        product_id=product_id,
        severity="warning",
        side=side,
    )


def _evaluate_quantity_parity_batch(
    observed_finding_factory: ObservedFindingFactory,
    checks: Sequence[CheckDefinition],
    *,
    reference_product_ids: Sequence[str] = (),
    migrated_product_ids: Sequence[str] = (),
    product_count: int = 1,
) -> RunResult:
    return evaluate_parity(
        reference_findings=[
            _quantity_finding(
                observed_finding_factory,
                product_id=product_id,
                side="reference",
            )
            for product_id in reference_product_ids
        ],
        migrated_findings=[
            _quantity_finding(
                observed_finding_factory,
                product_id=product_id,
                side="migrated",
            )
            for product_id in migrated_product_ids
        ],
        run=RunMetadata(
            run_id="run",
            source_snapshot_id="source-snapshot",
            product_count=product_count,
        ),
        checks=list(checks),
    )


def test_run_checks_emits_python_and_dsl_findings(
    context_factory: ContextFactory,
) -> None:
    contexts = [
        context_factory(
            code="python-product",
            product={
                "product_quantity": 100.0,
                "serving_quantity": 150.0,
                "labels_tags": [],
            },
        ),
        context_factory(
            code="dsl-product",
            product={
                "product_name": "",
                "quantity": "500 g",
                "product_quantity": 500.0,
                "labels_tags": [],
            },
        ),
    ]

    findings = _run_checks(contexts)
    keys = {(finding.product_id, finding.check_id) for finding in findings}

    assert ("python-product", "en:serving-quantity-over-product-quantity") in keys
    assert ("dsl-product", "en:product-name-to-be-completed") in keys


def test_run_checks_can_filter_to_active_check_ids(
    context_factory: ContextFactory,
) -> None:
    contexts = [
        context_factory(
            code="python-product",
            product={
                "product_quantity": 100.0,
                "serving_quantity": 150.0,
                "labels_tags": [],
                "product_name": "",
            },
        )
    ]

    findings = _run_checks(
        contexts,
        active_check_ids={"en:serving-quantity-over-product-quantity"},
    )

    assert [finding.check_id for finding in findings] == [
        "en:serving-quantity-over-product-quantity"
    ]


def test_run_checks_preserves_family_check_id_and_concrete_food_group_codes(
    context_factory: ContextFactory,
) -> None:
    findings = _run_checks(
        [
            context_factory(
                code="food-group-product",
                product={"food_groups_tags": ["en:beverages"]},
            )
        ],
        active_check_ids={
            "en:food-groups-${level}-known",
            "en:food-groups-${level}-unknown",
        },
    )

    assert {
        (finding.check_id, finding.emitted_code or finding.check_id, finding.severity)
        for finding in findings
    } == {
        ("en:food-groups-${level}-known", "en:food-groups-1-known", "info"),
        ("en:food-groups-${level}-unknown", "en:food-groups-2-unknown", "info"),
        ("en:food-groups-${level}-unknown", "en:food-groups-3-unknown", "info"),
    }


def test_run_checks_emits_family_variants_per_nutrient_set(
    context_factory: ContextFactory,
) -> None:
    findings = _run_checks(
        [
            context_factory(
                code="nutrition-product",
                nutrition={
                    "input_sets": [
                        {
                            "source": "packaging",
                            "preparation": "as_sold",
                            "per": "100g",
                            "nutrients": {
                                "energy-kcal": {
                                    "value": 100,
                                    "value_computed": 10,
                                }
                            },
                        }
                    ],
                    "aggregated_set": {
                        "nutrients": {
                            "energy-kcal": {
                                "value": 100,
                                "value_computed": 10,
                            }
                        }
                    },
                },
            )
        ],
        active_check_ids={
            "en:${set_id}-energy-value-in-${unit}-does-not-match-value-computed-from-other-nutrients"
        },
    )

    assert {
        (finding.check_id, finding.emitted_code or finding.check_id, finding.severity)
        for finding in findings
    } == {
        (
            "en:${set_id}-energy-value-in-${unit}-does-not-match-value-computed-from-other-nutrients",
            "en:nutrition-packaging-as-sold-100g-energy-value-in-kcal-does-not-match-value-computed-from-other-nutrients",
            "error",
        ),
        (
            "en:${set_id}-energy-value-in-${unit}-does-not-match-value-computed-from-other-nutrients",
            "en:nutrition-energy-value-in-kcal-does-not-match-value-computed-from-other-nutrients",
            "warning",
        ),
    }


def test_evaluate_parity_reports_missing_and_extra(
    observed_finding_factory: ObservedFindingFactory,
    default_check_catalog: CheckCatalog,
) -> None:
    reference_findings = [
        observed_finding_factory(
            check_id="en:quantity-not-recognized",
            product_id="123",
            severity="warning",
            side="reference",
        )
    ]
    migrated_findings = [
        observed_finding_factory(
            check_id="en:product-name-to-be-completed",
            product_id="456",
            severity="completeness",
            side="migrated",
        )
    ]

    result = evaluate_parity(
        reference_findings=reference_findings,
        migrated_findings=migrated_findings,
        run=RunMetadata(
            run_id="run",
            source_snapshot_id="source-snapshot",
            product_count=2,
        ),
        checks=default_check_catalog.checks,
    )

    quantity_check = next(
        check
        for check in result.checks
        if check.definition.id == "en:quantity-not-recognized"
    )
    missing_name_check = next(
        check
        for check in result.checks
        if check.definition.id == "en:product-name-to-be-completed"
    )

    assert quantity_check.passed is False
    assert quantity_check.missing_count == 1
    assert quantity_check.extra_count == 0
    assert len(quantity_check.missing) == 1
    assert len(quantity_check.extra) == 0
    assert missing_name_check.passed is False
    assert missing_name_check.missing_count == 0
    assert missing_name_check.extra_count == 1
    assert len(missing_name_check.missing) == 0
    assert len(missing_name_check.extra) == 1


@pytest.mark.parametrize(
    (
        "reference_count",
        "migrated_count",
        "expected_matched",
        "expected_missing",
        "expected_extra",
    ),
    [
        (0, 2, 0, 0, 2),
        (2, 1, 1, 1, 0),
    ],
    ids=["extra-duplicates-remain-distinct", "shared-multiplicity-matches-once"],
)
def test_evaluate_parity_accounts_for_duplicate_finding_multiplicity(
    observed_finding_factory: ObservedFindingFactory,
    default_checks_by_id: Mapping[str, CheckDefinition],
    reference_count: int,
    migrated_count: int,
    expected_matched: int,
    expected_missing: int,
    expected_extra: int,
) -> None:
    result = _evaluate_quantity_parity_batch(
        observed_finding_factory,
        [default_checks_by_id[_QUANTITY_CHECK_ID]],
        reference_product_ids=("123",) * reference_count,
        migrated_product_ids=("123",) * migrated_count,
    )

    quantity_check = result.checks[0]

    assert quantity_check.matched_count == expected_matched
    assert quantity_check.missing_count == expected_missing
    assert quantity_check.extra_count == expected_extra
    assert len(quantity_check.missing) == expected_missing
    assert len(quantity_check.extra) == expected_extra


def test_parity_accumulator_keeps_exact_counts_and_capped_examples(
    observed_finding_factory: ObservedFindingFactory,
    default_check_catalog: CheckCatalog,
) -> None:
    batch_one = _evaluate_quantity_parity_batch(
        observed_finding_factory,
        default_check_catalog.checks,
        reference_product_ids=("001", "002"),
        product_count=2,
    )
    batch_two = _evaluate_quantity_parity_batch(
        observed_finding_factory,
        default_check_catalog.checks,
        migrated_product_ids=("003",),
    )

    accumulator = RunResultAccumulator(
        max_examples_per_side=1,
        checks=default_check_catalog.checks,
    )
    accumulator.add_batch(batch_one)
    accumulator.add_batch(batch_two)
    result = accumulator.build_result(
        run=RunMetadata(
            run_id="run",
            source_snapshot_id="source-snapshot",
            product_count=3,
        ),
    )

    quantity_check = next(
        check
        for check in result.checks
        if check.definition.id == "en:quantity-not-recognized"
    )
    assert quantity_check.missing_count == 2
    assert quantity_check.extra_count == 1
    assert len(quantity_check.missing) == 1
    assert len(quantity_check.extra) == 1


def test_evaluate_parity_can_filter_to_active_checks(
    observed_finding_factory: ObservedFindingFactory,
    default_checks_by_id: Mapping[str, CheckDefinition],
) -> None:
    result = _evaluate_quantity_parity_batch(
        observed_finding_factory,
        [default_checks_by_id[_QUANTITY_CHECK_ID]],
        reference_product_ids=("123",),
    )

    assert [check.definition.id for check in result.checks] == [
        "en:quantity-not-recognized"
    ]


def test_parity_accumulator_keeps_only_active_checks(
    observed_finding_factory: ObservedFindingFactory,
    default_checks_by_id: Mapping[str, CheckDefinition],
) -> None:
    batch = _evaluate_quantity_parity_batch(
        observed_finding_factory,
        [default_checks_by_id[_QUANTITY_CHECK_ID]],
        reference_product_ids=("123",),
    )

    accumulator = RunResultAccumulator(
        max_examples_per_side=1,
        checks=[default_checks_by_id["en:quantity-not-recognized"]],
    )
    accumulator.add_batch(batch)
    result = accumulator.build_result(
        run=RunMetadata(
            run_id="run",
            source_snapshot_id="source-snapshot",
            product_count=1,
        ),
    )

    assert [check.definition.id for check in result.checks] == [
        "en:quantity-not-recognized"
    ]


def test_run_checks_with_evaluators_rejects_ambiguous_filter_inputs(
    context_factory: ContextFactory,
) -> None:
    with pytest.raises(ValueError, match="either check_evaluators or active_check_ids"):
        run_checks_with_evaluators(
            [context_factory()],
            check_evaluators={
                "en:serving-quantity-over-product-quantity": lambda context: []
            },
            options=CheckRunOptions(
                active_check_ids={"en:serving-quantity-over-product-quantity"},
                log_loaded=False,
                log_progress=False,
            ),
        )
