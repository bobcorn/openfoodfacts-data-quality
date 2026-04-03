[Back to documentation index](../index.md)

# Data contracts

These contracts define the main data boundaries between runtime layers.

## Contract map

```mermaid
flowchart LR
    subgraph INPUTS["Input contracts"]
        A["RawProductRow"]
        B["EnrichedSnapshotResult"]
    end

    subgraph REF["Reference contracts"]
        C["LegacyBackendResultEnvelope"]
        D["ReferenceResult"]
        C --> D
    end

    subgraph RT["Shared runtime"]
        E["NormalizedContext"]
        F["Finding"]
        E --> F
    end

    subgraph APP["Application models"]
        G["ObservedFinding"]
        H["RunCheckResult"]
        I["RunResult"]
        G --> H --> I
    end

    A --> E
    B --> E
    D --> B
    D --> G
    F --> G
```

## Input contracts

### RawProductRow

`RawProductRow` is the input contract for raw runs loaded from a DuckDB
[source snapshot](glossary.md#source-snapshot).

Use this contract when the selected checks can run from public product rows
alone.

Reference points:

- Canonical model: `src/openfoodfacts_data_quality/contracts/raw.py`
- Column anchor: `openfoodfacts_data_quality.raw_products.RAW_INPUT_COLUMNS`
- Related runtime surface: `raw_products`

Checks that only need public product fields can stay on this surface and avoid
enriched snapshots. In application runs, checks on this surface can still need
the [reference path](../explanation/reference-data-and-parity.md#why-the-reference-path-exists)
when strict comparison requires reference findings.

### EnrichedSnapshotResult

`EnrichedSnapshotResult` is the stable library contract for enriched inputs.

It wraps:

- a product `code`
- an `enriched_snapshot` payload with structured `product`, `flags`,
  `category_props`, and `nutrition` sections

In [application runs](../explanation/application-runs.md), the legacy backend
emits a versioned result envelope whose stable payload includes
`ReferenceResult.enriched_snapshot`. The application projects that validated
payload into `EnrichedSnapshotResult`.

## Input surfaces

[Input surfaces](../explanation/runtime-model.md#input-surfaces) describe two
execution situations:

- `raw_products`: The check can run from the public source snapshot alone.
- `enriched_products`: The check depends on stable enriched data that must be
  materialized or provided.

The chosen surface changes:

- which checks are eligible for a run
- whether the
  [reference path](../explanation/reference-data-and-parity.md#why-the-reference-path-exists)
  must run
- which normalized context fields are available

## Runtime contracts

### NormalizedContext

Checks do not consume raw rows or backend payloads directly. They consume
`NormalizedContext`.

`NormalizedContext` is the central shared runtime contract because it decouples
checks from input shapes tied to one source, lets raw and enriched runs share
one execution model, and defines the dotted paths that are valid for
[DSL](../explanation/migrated-checks.md#definition-languages) use and input
surface inference.

## Reference contracts

### LegacyBackendResultEnvelope

`LegacyBackendResultEnvelope` is the versioned result contract emitted across
the language boundary by the Perl wrapper.

It carries:

- `contract_kind`
- `contract_version`
- `reference_result`

Python validates this envelope before the application uses the underlying
`ReferenceResult` payload.

### ReferenceResult

The application
[reference path](../explanation/reference-data-and-parity.md#why-the-reference-path-exists)
returns `ReferenceResult`.

Fields:

- `code`
- `enriched_snapshot`
- `legacy_check_tags`

This contract is owned by the Python runtime even when the legacy backend
produces the payload.

## Output contracts

### Finding

`Finding` is the library output of the shared runtime.

### ObservedFinding

`ObservedFinding` is the comparison model used by
[strict comparison](../explanation/reference-data-and-parity.md#strict-comparison).
Reference and migrated outputs are adapted into this shape before comparison.

### RunCheckResult

`RunCheckResult` is the application result for one check. It records the check
definition, whether the check is `compared` or `runtime_only`, migrated counts,
and reference counts plus mismatch details when comparison applies.

### RunResult

`RunResult` is the overall application summary for one run. It drives the
[HTML report](report-artifacts.md#html-report), `run.json`,
[snippet artifacts](report-artifacts.md#snippetsjson), and JSON download
bundles.

`run.json` and `snippets.json` are versioned JSON artifacts. They carry root
`kind` and `schema_version` metadata around the serialized payload.

`snippets.json` records snippet provenance with `origin="implementation"` for
current repository code and `origin="legacy"` for matched legacy source spans.
Each check entry also records `legacy_snippet_status`.

## Stability

Treat these contracts as stable project boundaries.

Changes to them often affect
[check selection](check-metadata-and-selection.md#selection-inputs), context
projection, DSL validation,
[reference loading](../explanation/reference-data-and-parity.md#why-the-reference-path-exists),
[comparison behavior](../explanation/reference-data-and-parity.md#strict-comparison),
and [artifact generation](report-artifacts.md).

## See also

- [About the runtime model](../explanation/runtime-model.md)
- [About reference data and parity](../explanation/reference-data-and-parity.md)
- [Report artifacts](report-artifacts.md)

[Back to documentation index](../index.md)
