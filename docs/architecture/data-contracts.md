# Data Contracts

[Documentation](../index.md) / [Architecture](index.md) / Data Contracts

These contracts define how the main layers exchange data.

## Input Contracts

The repository has two input surfaces because checks do not all need the same kind of data.

- some checks can run directly on the raw public-product columns
- some checks need backend-derived data that only exists after enrichment

### Raw Source Rows

Raw runs start from public-product rows loaded from a DuckDB snapshot. The raw input contract is explicit and anchored by `openfoodfacts_data_quality.raw_products.RAW_INPUT_COLUMNS`.

### Enriched Snapshot

Enriched runs start from `EnrichedSnapshotResult`, which wraps:

- a product `code`
- an `enriched_snapshot` payload with product, flags, category props, and nutrition sections

This is the library-facing enriched contract. It is narrower than the full legacy backend internals.

The enriched surface is explicit today, but it is more sensitive than the raw surface because it depends on backend-derived data. Its long-term stability depends on which enriched fields are treated as part of the reusable contract.

## Input Surfaces

`raw_products` and `enriched_products` represent two execution situations.

- `raw_products` means the check can be evaluated from the public source snapshot alone
- `enriched_products` means the check depends on data that must first be materialized through the legacy backend boundary

This affects:

- which checks are eligible for a run
- whether the reference path must be activated
- which normalized-context fields are actually available

## Shared Runtime Contract

### NormalizedContext

Checks do not read raw rows or backend payloads directly. They read `NormalizedContext`.

`NormalizedContext` is the shared runtime contract because it:

- decouples checks from source-specific input shapes
- makes raw and enriched runs comparable
- defines which paths are legal for DSL and for each input surface

## Reference-Side Contract

### ReferenceResult

The legacy backend boundary returns `ReferenceResult`. It contains:

- `enriched_snapshot`
- `legacy_check_tags`

The backend output remains explicit. The Python side receives named fields rather than an opaque prepared payload.

## Output Contracts

### Finding

The library emits `Finding` objects. They are the public output of the shared check runtime.

### ObservedFinding

The parity application adapts both reference and migrated outputs into `ObservedFinding`, the common comparison shape.

### ParityResult

The application summarizes one run into `ParityResult`, which then drives:

- the HTML report
- `parity.json`
- snippet artifacts and download bundles

## Contract Stability

These contracts change more slowly than the surrounding code. They are the interfaces between the major layers.

[Back to Architecture](index.md) | [Back to Documentation](../index.md)
