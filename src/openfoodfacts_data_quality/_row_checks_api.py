from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING

from openfoodfacts_data_quality._provider_api import (
    list_provider_checks,
    run_provider_checks,
)
from openfoodfacts_data_quality.context.providers import SOURCE_PRODUCTS_PROVIDER
from openfoodfacts_data_quality.contracts.checks import CheckJurisdiction
from openfoodfacts_data_quality.contracts.findings import Finding
from openfoodfacts_data_quality.contracts.source_products import (
    SOURCE_PRODUCT_INPUT_COLUMNS,
)
from openfoodfacts_data_quality.source_product_preparation import (
    prepare_source_products,
)

if TYPE_CHECKING:
    from openfoodfacts_data_quality.checks.catalog import CheckCatalog
    from openfoodfacts_data_quality.contracts.checks import CheckDefinition


def list_row_checks(
    *,
    check_ids: Collection[str] | None = None,
    jurisdictions: Collection[CheckJurisdiction] | None = None,
    catalog: CheckCatalog | None = None,
) -> tuple[CheckDefinition, ...]:
    """Return checks available on the canonical row-based contract."""
    return list_provider_checks(
        provider=SOURCE_PRODUCTS_PROVIDER,
        check_ids=check_ids,
        jurisdictions=jurisdictions,
        catalog=catalog,
    )


def run_row_checks(
    rows: object,
    *,
    columns: Mapping[str, str] | None = None,
    check_ids: Collection[str] | None = None,
    jurisdictions: Collection[CheckJurisdiction] | None = None,
    catalog: CheckCatalog | None = None,
) -> list[Finding]:
    """Prepare one row stream and run the selected checks."""
    return run_provider_checks(
        prepare_source_products(rows, columns=columns),
        provider=SOURCE_PRODUCTS_PROVIDER,
        check_ids=check_ids,
        jurisdictions=jurisdictions,
        catalog=catalog,
    )


__all__ = [
    "SOURCE_PRODUCT_INPUT_COLUMNS",
    "list_row_checks",
    "run_row_checks",
]
