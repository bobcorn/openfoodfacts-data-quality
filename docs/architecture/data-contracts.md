# Data Contracts

[Documentation](../index.md) / [Architecture](index.md) / Data Contracts

These contracts are the main data boundaries between layers. They change more slowly than the surrounding orchestration code.

## Input Contracts

The check catalog spans two input surfaces because not every check needs the same source data.

### Raw Source Rows

Raw runs start from public product rows loaded from a DuckDB snapshot.

The raw input contract is explicit and anchored by `openfoodfacts_data_quality.raw_products.RAW_INPUT_COLUMNS`.

Checks that only need public product fields can remain on this surface and avoid the enrichment path entirely.

### Enriched Snapshot

Enriched runs start from `EnrichedSnapshotResult`, which wraps:

- a product `code`
- an `enriched_snapshot` payload with `product`, `flags`, `category_props`, and `nutrition`

This is the library contract for enriched inputs. It is narrower than the full legacy backend payload.

## Input Surfaces

`raw_products` and `enriched_products` describe two different execution situations:

- `raw_products`
  The check can run from the public source snapshot alone.
- `enriched_products`
  The check depends on data that must first be materialized through the legacy backend boundary.

This affects:

- which checks are eligible for a run
- whether the reference path must be activated
- which normalized context fields are available

## Shared Runtime Contract

### NormalizedContext

Checks do not consume raw rows or backend payloads directly. They consume `NormalizedContext`.

`NormalizedContext` is the central shared runtime contract because it:

- decouples checks from source specific input shapes
- lets raw and enriched runs share one execution model
- defines which dotted paths are legal for DSL use and for each input surface

The path metadata derived from the context contract also drives input surface inference for checks.

## Reference Side Contract

### ReferenceResult

The legacy backend boundary returns `ReferenceResult`.

Fields:

- `enriched_snapshot`
- `legacy_check_tags`

The boundary is explicit. The Python side receives named fields.

## Output Contracts

### Finding

`Finding` is the library output of the shared runtime.

### ObservedFinding

`ObservedFinding` is the comparison model used by parity. Both reference and migrated outputs are adapted into this shape before parity evaluation.

### ParityResult

`ParityResult` is the top level application summary for one run. It drives:

- the HTML report
- `parity.json`
- snippet artifacts and JSON download bundles

## Contract Stability

Treat these contracts as stable project boundaries.

Changes to them often have broad effects on:

- check selection
- context projection
- DSL validation
- reference loading
- artifact generation

## Next Reads

- [Check System](check-system.md)
- [Library Usage](../guides/library-usage.md)
- [Configuration and Artifacts](../operations/configuration-and-artifacts.md)
