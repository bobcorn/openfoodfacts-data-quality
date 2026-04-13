"""Public row-based API for listing, preparing, and running checks."""

from __future__ import annotations

from off_data_quality._row_checks_api import (
    SOURCE_PRODUCT_INPUT_COLUMNS,
    list_row_checks,
    run_row_checks,
)
from off_data_quality._source_product_preparation import (
    prepare_source_products,
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
prepare = prepare_source_products
run = run_row_checks

__all__ = [
    "COLUMNS",
    "CheckBinding",
    "CheckEvaluator",
    "SourceProduct",
    "check",
    "check_bindings",
    "list",
    "prepare",
    "run",
]
