[Back to documentation index](../index.md)

# Use the Python library

Use the Python library when you want findings without DuckDB loading, reference
caching, or report rendering.

## Choose a namespace

- `openfoodfacts_data_quality.raw`
- `openfoodfacts_data_quality.enriched`

Each namespace exposes `list_checks(...)` and `run_checks(...)`.

They map to the two
[input surfaces](../explanation/runtime-model.md#input-surfaces) supported by
the [shared runtime](../explanation/runtime-model.md#why-the-runtime-is-split).

## Run checks on raw rows

Use `raw` when the checks you care about can run from public product rows
alone.

Pass rows from Open Food Facts public source snapshots. The library normalizes
those rows into its internal raw runtime contract before it builds check
contexts.

That means callers can use rows loaded from:

- public Parquet snapshots
- DuckDB databases created from those public snapshots
- the public CSV export

The public CSV download is tab-separated even though it uses a `.csv` file
name.

The raw surface accepts the real public row shapes for those sources. It
handles the flat CSV export and the structured Parquet or DuckDB snapshot
shape internally.

```python
from openfoodfacts_data_quality import raw

findings = raw.run_checks(
    rows,
    check_ids=["en:serving-quantity-over-product-quantity"],
)
```

## Run checks on enriched snapshots

Use `enriched` when the checks need data derived from the backend, such as
flags, category properties, or richer nutrition structures.

Pass [EnrichedSnapshotResult](../reference/data-contracts.md#enrichedsnapshotresult)
items to the enriched surface:

```python
from openfoodfacts_data_quality import enriched

findings = enriched.run_checks(snapshots)
```

Library callers can build enriched snapshots directly without going through the
application
[reference path](../explanation/reference-data-and-parity.md#why-the-reference-path-exists).

## Narrow the active check set

- Pass `check_ids` to run a specific subset.
- Pass [`jurisdictions`](../reference/check-metadata-and-selection.md#metadata-fields)
  to limit eligible checks.

If you request checks that are not valid for the selected
[input surface](../explanation/runtime-model.md#input-surfaces), the library
fails explicitly.

## Know what the library does not do

The public library does not include:

- source loading from DuckDB
- reference result loading or caching
- report generation
- artifact serialization
- [strict comparison](../explanation/reference-data-and-parity.md#strict-comparison)
  orchestration in the application

Those responsibilities stay in the application layer.

## Choose the library or the application

Use the library for programmatic findings inside Python callers.

Use the application when you need
[source loading](../explanation/application-runs.md#run-overview),
[reference data](../explanation/reference-data-and-parity.md),
[strict comparison](../explanation/reference-data-and-parity.md#strict-comparison),
or the [HTML report](../reference/report-artifacts.md#html-report).

## Related information

- [About the runtime model](../explanation/runtime-model.md)
- [Data contracts](../reference/data-contracts.md)
- [Run the project locally](run-the-project-locally.md)

[Back to documentation index](../index.md)
