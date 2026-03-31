from __future__ import annotations

from types import MappingProxyType

import pytest

import openfoodfacts_data_quality
from openfoodfacts_data_quality import enriched, raw
from openfoodfacts_data_quality.checks.catalog import CheckCatalog
from openfoodfacts_data_quality.contracts.checks import CheckDefinition, CheckEmission
from openfoodfacts_data_quality.contracts.enrichment import EnrichedSnapshotResult


def _library_test_catalog() -> CheckCatalog:
    def warning_evaluator(_: object) -> list[CheckEmission]:
        return [CheckEmission(severity="warning")]

    global_check = CheckDefinition(
        id="en:global-check",
        definition_language="python",
        parity_baseline="legacy",
        jurisdictions=("global",),
        required_context_paths=("product.code",),
        supported_input_surfaces=("raw_products",),
    )
    canada_check = CheckDefinition(
        id="ca:runtime-check",
        definition_language="python",
        parity_baseline="none",
        jurisdictions=("ca",),
        required_context_paths=("product.code",),
        supported_input_surfaces=("raw_products",),
    )
    evaluators = {
        global_check.id: warning_evaluator,
        canada_check.id: warning_evaluator,
    }
    checks = (global_check, canada_check)
    return CheckCatalog(
        checks=checks,
        evaluators_by_id=MappingProxyType(evaluators),
        checks_by_id=MappingProxyType({check.id: check for check in checks}),
    )


def test_list_checks_defaults_to_raw_products_surface() -> None:
    check_ids = {check.id for check in raw.list_checks()}

    assert "en:created-missing" in check_ids
    assert "en:serving-quantity-over-product-quantity" in check_ids
    assert "en:main-language-code-missing" not in check_ids


def test_list_checks_defaults_to_all_jurisdictions() -> None:
    check_ids = {check.id for check in raw.list_checks(catalog=_library_test_catalog())}

    assert check_ids == {"en:global-check", "ca:runtime-check"}


def test_raw_surface_run_checks_executes_raw_products_checks_only() -> None:
    findings = raw.run_checks(
        [
            {
                "code": "123",
                "product_name": "Example",
                "quantity": "500 g",
                "product_quantity": "100",
                "serving_size": "150 g",
                "serving_quantity": "150",
                "brands": None,
                "categories": None,
                "labels": None,
                "emb_codes": None,
                "ingredients_text": None,
                "ingredients_tags": None,
                "nutriscore_grade": None,
                "nutriscore_score": None,
                "categories_tags": None,
                "labels_tags": None,
                "countries_tags": None,
                "fat_100g": None,
                "saturated-fat_100g": None,
                "trans-fat_100g": None,
                "sugars_100g": None,
                "fiber_100g": None,
                "omega-3-fat_100g": None,
                "energy-kcal_100g": None,
            }
        ],
        check_ids={"en:serving-quantity-over-product-quantity"},
    )

    assert [
        (finding.product_id, finding.check_id, finding.severity) for finding in findings
    ] == [("123", "en:serving-quantity-over-product-quantity", "warning")]


def test_raw_surface_run_checks_defaults_to_all_jurisdictions() -> None:
    findings = raw.run_checks(
        [{"code": "123"}],
        catalog=_library_test_catalog(),
    )

    assert {(finding.product_id, finding.check_id) for finding in findings} == {
        ("123", "en:global-check"),
        ("123", "ca:runtime-check"),
    }


def test_raw_surface_run_checks_can_filter_jurisdictions_with_a_list() -> None:
    findings = raw.run_checks(
        [{"code": "123"}],
        catalog=_library_test_catalog(),
        jurisdictions=["global"],
    )

    assert {(finding.product_id, finding.check_id) for finding in findings} == {
        ("123", "en:global-check"),
    }


def test_raw_surface_run_checks_rejects_enriched_only_checks() -> None:
    with pytest.raises(
        ValueError,
        match="Checks not available for input surface raw_products",
    ):
        raw.run_checks(
            [{"code": "123"}],
            check_ids={"en:main-language-code-missing"},
        )


def test_root_package_exposes_surface_namespaces() -> None:
    assert hasattr(openfoodfacts_data_quality, "raw")
    assert hasattr(openfoodfacts_data_quality, "enriched")
    assert not hasattr(openfoodfacts_data_quality, "list_checks")
    assert not hasattr(openfoodfacts_data_quality, "run_checks_on_rows")


def test_enriched_surface_exposes_snapshot_contract_types() -> None:
    assert hasattr(enriched, "EnrichedSnapshot")
    assert hasattr(enriched, "EnrichedSnapshotResult")


def test_enriched_surface_list_checks_includes_enriched_only_checks() -> None:
    check_ids = {check.id for check in enriched.list_checks()}

    assert "en:main-language-code-missing" in check_ids
    assert "en:ingredients-count-lower-than-expected-for-the-category" in check_ids


def test_enriched_surface_run_checks_executes_enriched_checks() -> None:
    findings = enriched.run_checks(
        [
            EnrichedSnapshotResult.model_validate(
                {
                    "code": "123",
                    "enriched_snapshot": {
                        "product": {
                            "code": "123",
                            "lang": "xx",
                            "labels_tags": [],
                        },
                        "flags": {},
                        "category_props": {},
                        "nutrition": {},
                    },
                }
            )
        ],
        check_ids={"en:main-language-unknown"},
    )

    assert [
        (finding.product_id, finding.check_id, finding.severity) for finding in findings
    ] == [("123", "en:main-language-unknown", "warning")]
