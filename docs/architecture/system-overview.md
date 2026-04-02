# System Overview

[Documentation](../index.md) / [Architecture](index.md) / System Overview

The repository has one shared check runtime and one [application run layer](application-run-flow.md) above it. See [Check System](check-system.md), [Data Contracts](data-contracts.md), and [Application Run Flow](application-run-flow.md).

## Split

`src/openfoodfacts_data_quality/` owns the reusable runtime.

`app/` owns orchestration, source loading, the [reference path](application-run-flow.md#reference-path), optional [strict comparison](application-run-flow.md#strict-comparison), and report generation.

`scripts/` owns operational workflows that support validation, sample refresh, and migration planning.

`app/` depends on `src/`. `src/` does not depend on `app/`.

## Shared Runtime

`src/openfoodfacts_data_quality/` provides:

- check contracts and metadata
- [normalized context](data-contracts.md#normalizedcontext) contracts
- packaged [Python and dsl checks](check-system.md)
- the [check catalog](check-system.md) and evaluator selection logic
- context building and projection
- public [`raw` and `enriched` Python APIs](../guides/library-usage.md)

This layer can be used without the application run layer.

## Application Layer

`app/` covers application execution and migration evaluation:

- [source snapshot](../glossary.md) loading from DuckDB
- reference loading that checks the cache first and uses the [legacy backend](application-run-flow.md#legacy-backend) on cache misses
- [reference result](data-contracts.md#referenceresult) caching, loading, envelope validation, and projection onto parity findings and enriched snapshots
- run result accumulation
- [strict comparison](application-run-flow.md#strict-comparison) where a legacy baseline exists
- report rendering, snippet extraction, and preview serving
- shared legacy source analysis reused by the report and inventory tooling

## Operations

The repository also includes workflows outside the core runtime:

- `scripts/validate_dsl.py` for structural and semantic DSL validation
- sample refresh scripts
- distribution build verification
- legacy inventory export and assessment application

## Repository Map

- `src/openfoodfacts_data_quality/checks/`
  Check definitions, DSL subsystem, registry helpers, catalog loading, and execution.
- `src/openfoodfacts_data_quality/context/`
  Context building, path metadata, and input projection into `NormalizedContext`.
- `src/openfoodfacts_data_quality/contracts/`
  Stable runtime contracts shared across the reusable library APIs.
- `app/source/`
  Source snapshot access helpers.
- `app/run/`
  Run preparation, batching, scheduling, run result accumulation, and end to end orchestration.
- `app/reference/`
  Reference side models, cache handling, result loading, envelope validation, materializers, and finding normalization.
- `app/legacy_backend/`
  The Perl runtime boundary and the persistent session pool that drives it.
- `app/legacy_source.py`
  Shared Tree-sitter source analysis for snippet extraction and inventory export.
- `app/parity/`
  Strict comparison logic between reference and migrated findings.
- `app/report/`
  Static report rendering, JSON download bundling, and snippet presentation.

## Placement

Concerns that exist independently of the application run layer usually belong in `src/`.

Concerns that exist to load source data, compare against the legacy backend, or produce review artifacts usually belong in `app/`.

## Next

- [Data Contracts](data-contracts.md)
- [Check System](check-system.md)
- [Application Run Flow](application-run-flow.md)
