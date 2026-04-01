# System Overview

[Documentation](../index.md) / [Architecture](index.md) / System Overview

The repository has one shared check runtime and one application focused on parity built on top of it.

## High Level Split

`src/openfoodfacts_data_quality/` owns the reusable runtime.

`app/` owns orchestration, reference loading, parity, and report generation.

`scripts/` owns operational workflows that support validation, sample refresh, and migration planning.

`app/` depends on `src/`. `src/` does not depend on `app/`.

## Shared Runtime

`src/openfoodfacts_data_quality/` provides:

- check contracts and metadata
- normalized context contracts
- packaged Python and DSL checks
- the check catalog and evaluator selection logic
- context building and projection
- public `raw` and `enriched` Python APIs

This layer can be used without the parity application.

## Application Layer

`app/` covers migration evaluation:

- source snapshot loading from DuckDB
- legacy backend input projection and execution
- reference result caching and loading
- parity comparison and accumulation
- report rendering, snippet extraction, and preview serving

## Operational Layer

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
- `app/pipeline/`
  Run preparation, batching, scheduling, and end to end execution orchestration.
- `app/reference/`
  Reference side models, cache handling, result loading, and finding normalization.
- `app/legacy_backend/`
  The Perl runtime boundary and the persistent session pool that drives it.
- `app/parity/`
  Comparison, accumulation, and serialization of parity results.
- `app/report/`
  Static report rendering, JSON download bundling, and code snippet extraction.

## Placement Rule

Concerns that exist independently of parity usually belong in `src/`.

Concerns that exist to compare against the legacy backend or to produce migration review artifacts usually belong in `app/`.

## Next Reads

- [Data Contracts](data-contracts.md)
- [Check System](check-system.md)
- [Parity Pipeline](parity-pipeline.md)
