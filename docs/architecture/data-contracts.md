# Data Contracts

[Documentation](../index.md) / [Architecture](index.md) / Data Contracts

These contracts are the main data boundaries between layers. They change more slowly than the surrounding orchestration code.

## Input Contracts

The check catalog spans two input surfaces because not every check needs the same source data.

### Raw Source Rows

Raw runs start from `RawProductRow` objects loaded from a DuckDB snapshot.

The raw input contract is explicit. Its canonical model lives in `src/openfoodfacts_data_quality/contracts/raw.py`, and its OFF column names are anchored by `openfoodfacts_data_quality.raw_products.RAW_INPUT_COLUMNS`.

Checks that only need public product fields can remain on this surface and avoid the enrichment path entirely.

### Enriched Snapshot

Enriched runs start from `EnrichedSnapshotResult`, which wraps:

- a product `code`
- an `enriched_snapshot` payload with structured `product`, `flags`, `category_props`, and `nutrition` sections

This is the stable library contract for enriched inputs, owned by the Python runtime.

In application runs, the legacy backend emits a versioned result envelope whose stable payload includes `ReferenceResult.enriched_snapshot`. The application then projects that validated payload into `EnrichedSnapshotResult`.

## Input Surfaces

`raw_products` and `enriched_products` describe two different execution situations:

- `raw_products`
  The check can run from the public source snapshot alone.
- `enriched_products`
  The check depends on stable enriched data. In application runs, that data is materialized through the legacy backend boundary and then projected onto the enriched contract owned by the Python runtime. In library usage, callers can provide explicit enriched snapshots directly.

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

The application reference path returns `ReferenceResult`.

Fields:

- `code`
- `enriched_snapshot`
- `legacy_check_tags`

The cross language boundary is explicit. The legacy backend emits `LegacyBackendResultEnvelope`, which carries `contract_kind`, `contract_version`, and a stable `reference_result` payload. Python validates that envelope and then works with `ReferenceResult`.

That keeps the public enriched and reference contracts owned by Python instead of by the Perl boundary details.

## Output Contracts

### Finding

`Finding` is the library output of the shared runtime.

### ObservedFinding

`ObservedFinding` is the comparison model used by strict comparison. Both reference and migrated outputs are adapted into this shape before comparison.

### RunCheckResult

`RunCheckResult` is the application result for one check. It records:

- the check definition
- whether the check is `compared` or `runtime_only`
- migrated counts
- reference counts and mismatch details when comparison applies

### RunResult

`RunResult` is the overall application summary for one run. It drives:

- the HTML report
- `run.json`
- snippet artifacts and JSON download bundles

`run.json` and `snippets.json` are versioned JSON artifacts. They carry root `kind` and `schema_version` metadata around the serialized payload.
`snippets.json` records snippet provenance with `origin="implementation"` for current repository code and `origin="legacy"` for matched legacy source spans. Each check entry also records `legacy_snippet_status` as `available`, `not_applicable`, or `unavailable`.

## Contract Stability

Treat these contracts as stable project boundaries.

Changes to them often have broad effects on:

- check selection
- context projection
- DSL validation
- reference loading
- comparison behavior
- artifact generation

## Next Reads

- [Check System](check-system.md)
- [Library Usage](../guides/library-usage.md)
- [Configuration and Artifacts](../operations/configuration-and-artifacts.md)
