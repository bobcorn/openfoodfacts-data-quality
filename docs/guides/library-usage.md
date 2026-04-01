# Library Usage

[Documentation](../index.md) / [Guides](index.md) / Library Usage

The public Python API is organized by input surface rather than by parity workflow.

## Public Entry Points

- `openfoodfacts_data_quality.raw`
- `openfoodfacts_data_quality.enriched`

Both surfaces expose:

- `list_checks(...)`
- `run_checks(...)`

The root package intentionally exposes these two namespaces rather than one flat API.

## Raw Surface

Use `raw` when the checks you care about can be decided from public-product rows alone.

Those rows should match the explicit raw source contract anchored by `openfoodfacts_data_quality.raw_products.RAW_INPUT_COLUMNS`.

```python
from openfoodfacts_data_quality import raw

findings = raw.run_checks(
    rows,
    check_ids=["en:serving-quantity-over-product-quantity"],
)
```

## Enriched Surface

Use `enriched` when the checks need backend-derived fields such as:

- enriched flags
- category properties
- richer nutrition structures

The input items are `EnrichedSnapshotResult` objects.

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

- the parity application
- the reference result model
- report generation
- reference result caching
- the internal details of normalized context construction

Those remain application concerns.

## Runtime-Only Checks

The library can execute both parity-backed and runtime-only checks.

That is broader than the current migration report flow, which is still focused on parity-compared checks. If you only need programmatic findings and not the full migration-report application, the library APIs are the right entrypoint.

## Next Reads

- [Data Contracts](../architecture/data-contracts.md)
- [Check System](../architecture/check-system.md)
- [Troubleshooting](../getting-started/troubleshooting.md)
