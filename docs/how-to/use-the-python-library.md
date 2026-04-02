# Use the Python Library

[Back to documentation](../index.md)

Call the Python library when you want findings without DuckDB loading, reference caching, or report rendering.

## Choose a namespace

- `openfoodfacts_data_quality.raw`
- `openfoodfacts_data_quality.enriched`

Both namespaces expose `list_checks(...)` and `run_checks(...)`.

## Run checks on raw rows

Use `raw` when the checks you care about can run from public product rows alone.

Those rows should match the explicit [raw input contract](../reference/data-contracts.md#raw-rows) anchored by `openfoodfacts_data_quality.raw_products.RAW_INPUT_COLUMNS`.

```python
from openfoodfacts_data_quality import raw

findings = raw.run_checks(
    rows,
    check_ids=["en:serving-quantity-over-product-quantity"],
)
```

## Run checks on enriched snapshots

Use `enriched` when the checks need data derived from the backend, such as flags, category properties, or richer nutrition structures.

Pass [`EnrichedSnapshotResult`](../reference/data-contracts.md#enriched-snapshot) items to the enriched surface:

```python
from openfoodfacts_data_quality import enriched

findings = enriched.run_checks(snapshots)
```

Library callers can build enriched snapshots directly without going through the application [reference path](../concepts/reference-and-parity.md#reference-path).

## Narrow selection

- pass `check_ids` to run a specific subset
- pass [`jurisdictions`](../reference/check-metadata-and-selection.md#metadata-fields) to limit eligible checks

If you request checks that are not valid for the selected [input surface](../concepts/runtime-model.md#input-surfaces), the library fails explicitly.

## Know what the library does not do

The public library does not include:

- source loading from DuckDB
- reference result loading or caching
- report generation
- artifact serialization
- strict comparison orchestration in the application

Those responsibilities stay in the application layer.

## Choose library or application runs

Use the library for programmatic findings inside Python callers.

Use the application when you need [source loading](../concepts/how-an-application-run-works.md), [reference data](../concepts/reference-and-parity.md#reference-data), [strict comparison](../concepts/reference-and-parity.md#strict-comparison), or the [HTML report](../reference/report-artifacts.md#html-report).

[Back to documentation](../index.md)
