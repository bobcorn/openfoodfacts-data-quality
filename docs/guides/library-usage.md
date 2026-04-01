# Library Usage

[Documentation](../index.md) / [Guides](index.md) / Library Usage

The public Python API is organized by input surface.

## Public APIs

- `openfoodfacts_data_quality.raw`
- `openfoodfacts_data_quality.enriched`

Each surface exposes:

- `list_checks(...)`
- `run_checks(...)`

The root package exposes these two namespaces.

## Raw Surface

Use `raw` when the checks you care about can be decided from public product rows alone.

Those rows should match the explicit raw source contract anchored by `openfoodfacts_data_quality.raw_products.RAW_INPUT_COLUMNS`.

```python
from openfoodfacts_data_quality import raw

findings = raw.run_checks(
    rows,
    check_ids=["en:serving-quantity-over-product-quantity"],
)
```

## Enriched Surface

Use `enriched` when the checks need fields derived from the backend, such as:

- enriched flags
- category properties
- richer nutrition structures

The input items are `EnrichedSnapshotResult` objects.

This is a stable input contract owned by the Python runtime, with structured `product`, `flags`, `category_props`, and `nutrition` sections. Library callers can build these snapshots directly without going through the application reference path.

```python
from openfoodfacts_data_quality import enriched

findings = enriched.run_checks(snapshots)
```

## Selection Parameters

Both surfaces support:

- `check_ids` to narrow execution to specific checks
- `jurisdictions` to restrict the visible checks

If you request checks that are not valid for the selected surface, the library fails explicitly instead of silently skipping them.

## Public API Boundaries

The public library APIs do not expose:

- the application run layer
- the reference result model
- report generation
- reference result caching
- the internal details of normalized context construction

Those remain application concerns.

## Runtime Only Checks

The library can execute checks compared against legacy behavior. It can also execute runtime only checks.

The application run layer supports the same model, and shipped profiles can mix compared and runtime only checks in one run. The library remains the simpler entry point when you only need programmatic findings without source loading, reference loading, or report generation.

## Next Reads

- [Data Contracts](../architecture/data-contracts.md)
- [Check System](../architecture/check-system.md)
- [Troubleshooting](../getting-started/troubleshooting.md)
