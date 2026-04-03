[Back to documentation index](../index.md)

# About the system architecture

This page explains how repository responsibilities are split between `src/` and
`app/`.

## Repository split

```mermaid
flowchart LR
    subgraph SRC["src/openfoodfacts_data_quality/"]
        A["Contracts and context"]
        B["Check catalog and runtime"]
        C["Public library APIs"]
        A --> B --> C
    end

    subgraph APP["app/"]
        D["Source and run"]
        E["Reference and parity"]
        F["Report"]
        D --> E --> F
    end

    G["Legacy backend"]

    B --> D
    E -.-> G
```

`src/openfoodfacts_data_quality/` owns the
[shared runtime](runtime-model.md#why-the-runtime-is-split).

`app/` owns orchestration, source loading, the
[reference path](reference-data-and-parity.md#why-the-reference-path-exists),
optional [strict comparison](reference-data-and-parity.md#strict-comparison),
and report generation.

`app/` depends on `src/`. `src/` does not depend on `app/`.

## Shared runtime responsibilities

`src/openfoodfacts_data_quality/` provides:

- check contracts and
  [metadata](../reference/check-metadata-and-selection.md)
- [`NormalizedContext`](runtime-model.md#normalizedcontext) contracts
- packaged Python and DSL checks
- catalog loading and evaluator selection
- context building and projection
- public [`raw` and `enriched` Python APIs](../how-to/use-the-python-library.md)

## Application responsibilities

`app/` provides:

- [source snapshot](../reference/glossary.md#source-snapshot) loading from
  DuckDB
- reference loading through the
  [reference path](reference-data-and-parity.md#why-the-reference-path-exists),
  with
  [reference result cache](../reference/run-configuration-and-artifacts.md#reference-result-cache)
  reuse first and backend materialization on cache misses
- [ReferenceResult](../reference/data-contracts.md#referenceresult) caching,
  loading, envelope validation, and projection onto reference findings plus
  enriched snapshots
- [RunResult](../reference/data-contracts.md#runresult) accumulation
- [strict comparison](reference-data-and-parity.md#strict-comparison)
- [report rendering](../reference/report-artifacts.md#html-report), snippet
  extraction, and local preview

## Repository map

- `src/openfoodfacts_data_quality/checks/`: Check definitions, the DSL
  subsystem, registry helpers, catalog loading, and execution.
- `src/openfoodfacts_data_quality/context/`: Context building, path metadata,
  and input projection into `NormalizedContext`.
- `src/openfoodfacts_data_quality/contracts/`: Stable runtime contracts shared
  across the reusable library APIs.
- `app/source/`: Source snapshot access helpers.
- `app/run/`: Run preparation, batching, scheduling, run result accumulation,
  and full application orchestration.
- `app/reference/`: Runtime data for the reference side, cache handling, result
  loading, envelope validation, materializers, and finding normalization.
- `app/legacy_backend/`: The Perl runtime boundary and the persistent session
  pool that drives it.
- `app/legacy_source.py`: Shared Tree-sitter source analysis for report
  snippets.
- `app/parity/`: Strict comparison logic between reference and migrated
  findings.
- `app/report/`: Static report rendering, JSON download bundling, and snippet
  presentation.

## Boundary rules

Put reusable execution behavior in `src/`.

Put source loading, legacy backend integration,
[strict comparison](reference-data-and-parity.md#strict-comparison), and
[review artifacts](../reference/report-artifacts.md) in `app/`.

## Related information

- [About the runtime model](runtime-model.md)
- [About application runs](application-runs.md)
- [About the project scope](project-scope.md)

[Back to documentation index](../index.md)
