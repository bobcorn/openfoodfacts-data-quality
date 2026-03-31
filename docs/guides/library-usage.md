# Library Usage

[Documentation](../index.md) / [Guides](index.md) / Library Usage

The public Python API is organized by input surface, not by parity workflow.

## Public Entry Points

- `openfoodfacts_data_quality.raw`
- `openfoodfacts_data_quality.enriched`

Each surface exposes:

- `list_checks(...)`
- `run_checks(...)`

## Input Surfaces

- use `raw` when the check can be decided from the raw public-product columns
- use `enriched` when the check needs backend-derived fields that are only available after enrichment

## Raw Surface

Use `raw` when you have public-product rows that match the raw source contract.

```python
from openfoodfacts_data_quality import raw

findings = raw.run_checks(
    rows,
    check_ids=["en:serving-quantity-over-product-quantity"],
)
```

## Enriched Surface

Use `enriched` when you already have explicit `EnrichedSnapshotResult` items.

```python
from openfoodfacts_data_quality import enriched

findings = enriched.run_checks(snapshots)
```

Choose `enriched` when the checks you care about depend on fields such as backend-derived flags, category properties, or richer nutrition structures.

## Selection Parameters

Both surfaces support:

- `check_ids` to narrow execution to specific checks
- `jurisdictions` to limit the visible checks

## What The Public API Does Not Expose

The public library surface does not expose:

- the parity application
- the reference result model
- report generation
- the internal normalized-context construction details

Those are application concerns, not library concerns.

[Back to Guides](index.md) | [Back to Documentation](../index.md)
