"""Public row-based API for running checks on canonical Open Food Facts rows."""

from __future__ import annotations

from openfoodfacts_data_quality._row_checks_api import (
    SOURCE_PRODUCT_INPUT_COLUMNS,
    list_row_checks,
    run_row_checks,
)
from openfoodfacts_data_quality.contracts.source_products import SourceProduct

COLUMNS: tuple[str, ...] = SOURCE_PRODUCT_INPUT_COLUMNS

list = list_row_checks
run = run_row_checks

__all__ = [
    "COLUMNS",
    "SourceProduct",
    "list",
    "run",
]
