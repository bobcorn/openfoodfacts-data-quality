from __future__ import annotations

from types import MappingProxyType

import pytest

import off_data_quality
from off_data_quality import checks, snapshots
from off_data_quality.catalog import CheckCatalog
from off_data_quality.contracts.checks import CheckDefinition, CheckEmission


def _library_test_catalog() -> CheckCatalog:
    def warning_evaluator(_: object) -> list[CheckEmission]:
        return [CheckEmission(severity="warning")]

    global_check = CheckDefinition(
        id="en:global-check",
        definition_language="python",
        parity_baseline="legacy",
        jurisdictions=("global",),
        required_context_paths=("product.code",),
    )
    canada_check = CheckDefinition(
        id="ca:runtime-check",
        definition_language="python",
        parity_baseline="none",
        jurisdictions=("ca",),
        required_context_paths=("product.code",),
    )
    evaluators = {
        global_check.id: warning_evaluator,
        canada_check.id: warning_evaluator,
    }
    check_definitions = (global_check, canada_check)
    return CheckCatalog(
        checks=check_definitions,
        evaluators_by_id=MappingProxyType(evaluators),
        checks_by_id=MappingProxyType({check.id: check for check in check_definitions}),
    )


def test_checks_list_defaults_to_row_based_api() -> None:
    check_ids = {check.id for check in checks.list()}

    assert "en:created-missing" in check_ids
    assert "en:serving-quantity-over-product-quantity" in check_ids
    assert "en:main-language-code-missing" not in check_ids


def test_checks_list_defaults_to_all_jurisdictions() -> None:
    check_ids = {check.id for check in checks.list(catalog=_library_test_catalog())}

    assert check_ids == {"en:global-check", "ca:runtime-check"}


def test_checks_run_executes_row_based_checks_only() -> None:
    findings = checks.run(
        [
            {
                "code": "123",
                "product_name": "Example",
                "quantity": "500 g",
                "product_quantity": "100",
                "serving_size": "150 g",
                "serving_quantity": "150",
                "ingredients_text": "Sugar, salt",
                "ingredients_tags": "en:sugar,en:salt",
                "categories_tags": "en:dietary-supplements",
                "labels_tags": "en:vegan",
                "countries_tags": "en:canada",
                "energy-kcal_100g": "123",
                "fat_100g": "3.5",
            }
        ],
        check_ids={"en:serving-quantity-over-product-quantity"},
    )

    assert [
        (finding.product_id, finding.check_id, finding.severity) for finding in findings
    ] == [("123", "en:serving-quantity-over-product-quantity", "warning")]


def test_checks_run_accepts_explicit_column_remapping() -> None:
    findings = checks.run(
        [
            {
                "barcode": "123",
                "name": "Example",
                "qty": "500 g",
                "product_qty": "100",
                "serving_qty": "150",
                "serving_label": "150 g",
            }
        ],
        columns={
            "code": "barcode",
            "product_name": "name",
            "quantity": "qty",
            "product_quantity": "product_qty",
            "serving_quantity": "serving_qty",
            "serving_size": "serving_label",
        },
        check_ids={"en:serving-quantity-over-product-quantity"},
    )

    assert [
        (finding.product_id, finding.check_id, finding.severity) for finding in findings
    ] == [("123", "en:serving-quantity-over-product-quantity", "warning")]


def test_checks_run_defaults_to_all_jurisdictions() -> None:
    findings = checks.run(
        [{"code": "123"}],
        catalog=_library_test_catalog(),
    )

    assert {(finding.product_id, finding.check_id) for finding in findings} == {
        ("123", "en:global-check"),
        ("123", "ca:runtime-check"),
    }


def test_checks_run_can_filter_jurisdictions_with_a_list() -> None:
    findings = checks.run(
        [{"code": "123"}],
        catalog=_library_test_catalog(),
        jurisdictions=["global"],
    )

    assert {(finding.product_id, finding.check_id) for finding in findings} == {
        ("123", "en:global-check"),
    }


def test_checks_run_rejects_enriched_only_checks() -> None:
    with pytest.raises(
        ValueError,
        match="Checks not available for context provider source_products",
    ):
        checks.run(
            [{"code": "123"}],
            check_ids={"en:main-language-code-missing"},
        )


def test_checks_run_ignores_extra_columns() -> None:
    findings = checks.run(
        [{"code": "123", "unknown_column": "value"}],
        catalog=_library_test_catalog(),
        check_ids={"en:global-check"},
    )

    assert [(finding.product_id, finding.check_id) for finding in findings] == [
        ("123", "en:global-check")
    ]


def test_checks_run_rejects_unknown_canonical_columns_mapping() -> None:
    with pytest.raises(
        ValueError,
        match="unknown canonical columns in columns=: unknown_column",
    ):
        checks.run(
            [{"code": "123"}],
            columns={"unknown_column": "whatever"},
        )


def test_checks_run_rejects_missing_mapped_source_columns() -> None:
    with pytest.raises(
        ValueError,
        match="missing mapped source column 'barcode'",
    ):
        checks.run(
            [{"code": "123"}],
            columns={"code": "barcode"},
        )


def test_checks_run_rejects_file_paths() -> None:
    with pytest.raises(TypeError, match="does not read files"):
        checks.run("products.parquet")


def test_checks_run_accepts_pandas_like_tables() -> None:
    _assert_global_check_runs(_PandasLikeTable([_example_row()]))


def test_checks_run_accepts_pyarrow_like_tables() -> None:
    _assert_global_check_runs(_PyArrowLikeTable([_example_row()]))


def test_checks_run_accepts_duckdb_like_relations() -> None:
    _assert_global_check_runs(
        _DuckDBLikeRelation(
            columns=("code", "product_name"),
            records=[("123", "Example")],
        )
    )


def test_checks_run_accepts_openfoodfacts_product_export_rows() -> None:
    findings = checks.run(
        [
            {
                "code": "123",
                "created_t": 123,
                "product_name": [
                    {"lang": "fr", "text": "Exemple"},
                    {"lang": "main", "text": "Example"},
                ],
                "quantity": "500 g",
                "product_quantity": "500",
                "serving_size": "50 g",
                "serving_quantity": "50",
                "brands": "Brand",
                "categories": "Supplements",
                "labels": "No gluten",
                "emb_codes": "FR 01.001",
                "ingredients_text": [{"lang": "main", "text": "Sugar, salt"}],
                "ingredients_tags": ["en:sugar", "en:salt"],
                "nutriscore_grade": "a",
                "nutriscore_score": -2,
                "categories_tags": ["en:supplements"],
                "labels_tags": ["en:vegan"],
                "countries_tags": ["en:france"],
                "no_nutrition_data": False,
                "nutriments": [
                    {"name": "energy-kcal", "100g": 123.0},
                    {"name": "fat", "100g": 3.5},
                    {"name": "unsupported-nutrient", "100g": 9.0},
                ],
            }
        ],
        catalog=_library_test_catalog(),
        check_ids={"en:global-check"},
    )

    assert [(finding.product_id, finding.check_id) for finding in findings] == [
        ("123", "en:global-check")
    ]


def test_off_data_quality_root_exposes_checks_and_snapshots() -> None:
    assert hasattr(off_data_quality, "checks")
    assert hasattr(off_data_quality, "snapshots")


def test_checks_namespace_exposes_canonical_columns() -> None:
    assert "code" in checks.COLUMNS
    assert "product_name" in checks.COLUMNS
    assert "energy-kcal_100g" in checks.COLUMNS


def test_snapshots_namespace_is_a_placeholder() -> None:
    assert "placeholder" in (snapshots.__doc__ or "").lower()
    assert not hasattr(snapshots, "run")


class _PandasLikeTable:
    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records

    def to_dict(self, *, orient: str) -> object:
        if orient != "records":
            raise ValueError("unsupported orient")
        return self._records


class _PyArrowLikeTable:
    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records

    def to_pylist(self) -> object:
        return self._records


class _DuckDBLikeRelation:
    def __init__(
        self,
        *,
        columns: tuple[str, ...],
        records: list[tuple[object, ...]],
    ) -> None:
        self.columns = columns
        self._records = records

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._records


def _example_row() -> dict[str, object]:
    return {
        "code": "123",
        "product_name": "Example",
    }


def _assert_global_check_runs(rows: object) -> None:
    findings = checks.run(
        rows,
        catalog=_library_test_catalog(),
        check_ids={"en:global-check"},
    )

    assert [(finding.product_id, finding.check_id) for finding in findings] == [
        ("123", "en:global-check")
    ]
