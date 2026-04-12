"""Public row-based API for running checks on canonical Open Food Facts rows."""

from __future__ import annotations

from off_data_quality._row_checks_api import (
    SOURCE_PRODUCT_INPUT_COLUMNS,
    list_row_checks,
    run_row_checks,
)
from off_data_quality._source_product_preparation import (
    OFF_PRODUCT_EXPORT_COLUMNS,
    prepare_source_products,
    project_off_product_export_row,
    project_off_product_export_rows,
)
from off_data_quality.checks._registry import (
    CheckBinding,
    CheckEvaluator,
    check,
    check_bindings,
)
from off_data_quality.contracts.source_products import SourceProduct

COLUMNS: tuple[str, ...] = SOURCE_PRODUCT_INPUT_COLUMNS

list = list_row_checks
run = run_row_checks

__all__ = [
    "COLUMNS",
    "CheckBinding",
    "CheckEvaluator",
    "OFF_PRODUCT_EXPORT_COLUMNS",
    "SourceProduct",
    "check",
    "check_bindings",
    "list",
    "prepare_source_products",
    "project_off_product_export_rows",
    "project_off_product_export_row",
    "run",
]
