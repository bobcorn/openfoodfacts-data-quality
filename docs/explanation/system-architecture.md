[Back to documentation index](../index.md)

# About the system architecture

The repository has one reusable layer in `src/` and one application layer in
`app/`.

## Project overview

```mermaid
flowchart TB
    subgraph SHARED["Shared Check System"]
        A["Python Checks"]
        B["DSL Checks"]
        C["Check Catalog and Metadata"]
        D["Shared Runtime"]
        A --> C
        B --> C
        C --> D
    end

    subgraph LIB["Python Library"]
        E["Checks API"]
        F["Snapshots API Placeholder"]
    end

    subgraph APP["Parity Application"]
        G["Source Snapshot Loading"]
        H["Reference Path"]
        I["Migrated Checks"]
        J["Strict Comparison, Storage, and Report"]
        G --> H
        G --> I
        H --> J
        I --> J
    end

    K["Legacy Backend"]

    D --> E
    D -.-> F
    D --> I
    K -.-> H
```

`src/openfoodfacts_data_quality/` owns the
[shared runtime](runtime-model.md#why-the-runtime-is-split).

In the diagram, `Checks API` refers to `off_data_quality.checks`. `Snapshots API
Placeholder` refers to the reserved `off_data_quality.snapshots` namespace.

`app/` owns orchestration, source loading, dataset selection, the
[reference path](reference-data-and-parity.md#why-the-reference-path-exists),
optional [strict comparison](reference-data-and-parity.md#strict-comparison),
stored review data, and report generation.

`app/` depends on `src/`. `src/` does not depend on `app/`.

## Shared runtime responsibilities

`src/openfoodfacts_data_quality/` provides:

- check contracts and
  [metadata](../reference/check-metadata-and-selection.md)
- [`CheckContext`](runtime-model.md#checkcontext) contracts
- packaged Python and DSL checks
- catalog loading and evaluator selection
- context building and projection
- the [`checks` Python API](../how-to/use-the-python-library.md) and the
  reserved `snapshots` placeholder namespace

## Application responsibilities

`app/` provides:

- [source snapshot](../reference/glossary.md#source-snapshot) loading from
  JSONL or DuckDB
- dataset profile resolution and row selection for one application run
- reference loading through the
  [reference path](reference-data-and-parity.md#why-the-reference-path-exists),
  with
  [reference result cache](../reference/run-configuration-and-artifacts.md#reference-result-cache)
  reuse first and backend materialization on cache misses
- [ReferenceResult](../reference/data-contracts.md#referenceresult) caching,
  loading, envelope validation, and projection onto reference findings plus
  reference check contexts
- [RunResult](../reference/data-contracts.md#runresult) accumulation
- migration catalog loading for planning metadata and profile filtering
- [strict comparison](reference-data-and-parity.md#strict-comparison)
- parity store persistence for run telemetry, mismatches, and review metadata
- [report rendering](../reference/report-artifacts.md#html-report), snippet
  extraction, and local preview

## Repository map

- `src/openfoodfacts_data_quality/checks/`: Check definitions, the DSL
  subsystem, registry helpers, catalog loading, and execution.
- `src/openfoodfacts_data_quality/context/`: Context building, path metadata,
  and input projection into `CheckContext`.
- `src/openfoodfacts_data_quality/contracts/`: Stable runtime contracts shared
  across the reusable library APIs.
- `app/application.py`: Application service that executes one run and renders
  the review site.
- `app/artifacts.py`: Artifact workspace preparation for `artifacts/latest/`.
- `app/source/`: Product document source loading and dataset profile helpers.
- `app/run/`: Run settings, profile loading, preparation, batching, scheduling,
  accumulation, serialization, and orchestration.
- `app/reference/`: Runtime data for the reference side, cache handling, result
  loading, envelope validation, materializers, and finding normalization.
- `app/legacy_backend/`: The Perl runtime boundary and the persistent session
  pool that drives it.
- `app/legacy_source.py`: Legacy source analysis used for snippet provenance
  and inventory export workflows.
- `app/migration/`: Migration family catalog loading and planning metadata used
  by run selection and review.
- `app/parity/`: Strict comparison logic.
- `app/storage/`: Application-owned persistence for recorded runs and parity
  review state.
- `app/report/`: Static report rendering, JSON download bundling, and snippet
  presentation.
- `config/check-profiles.toml`: Named check presets.
- `config/dataset-profiles.toml`: Named dataset presets for source selection.

## Boundary rules

Put reusable execution behavior in `src/`.

Put source loading, dataset selection, legacy backend integration,
[strict comparison](reference-data-and-parity.md#strict-comparison), mismatch
governance, review persistence, and
[report artifacts](../reference/report-artifacts.md) in `app/`.

## Related information

- [About the runtime model](runtime-model.md)
- [About application runs](application-runs.md)
- [About the project scope](project-scope.md)

[Back to documentation index](../index.md)
