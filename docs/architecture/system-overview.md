# System Overview

[Documentation](../index.md) / [Architecture](index.md) / System Overview

The repository is organized around one shared check system and two top-level runtime surfaces.

## Shared Runtime

`src/openfoodfacts_data_quality/` owns the reusable parts:

- check contracts
- normalized context contracts
- packaged Python and DSL check definitions
- check catalog and evaluator selection
- context building
- public `raw` and `enriched` APIs

This layer can be used without the parity application.

## Application Layer

`app/` owns the parts used for orchestration, parity, and reporting:

- source snapshot loading
- legacy backend input projection and execution
- reference result caching and loading
- parity comparison and accumulation
- report rendering and preview

`app/` depends on `src/`. `src/` does not depend on `app/`.

## Operational Layer

The repository also contains workflow-specific tooling:

- `scripts/` for validation, sample refresh, and planning tasks
- `config/check-profiles.toml` for named application run profiles
- `examples/` for small library-facing examples

## Repository Map

- `src/openfoodfacts_data_quality/checks/`
  Check definitions and the catalog live here. The package also contains the execution engine, DSL subsystem, and registry support.
- `src/openfoodfacts_data_quality/context/`
  Normalized context building and path metadata.
- `src/openfoodfacts_data_quality/contracts/`
  Stable runtime contracts consumed by checks and callers.
- `app/pipeline/`
  Run preparation, batching, and execution orchestration.
- `app/reference/`
  Reference-side loading, normalization, and caching.
- `app/parity/`
  Parity-domain models live here. The package also contains comparison, accumulation, and serialization code.
- `app/report/`
  HTML report generation, downloads, and snippet extraction.
- `app/legacy_backend/`
  This is the Perl boundary. It contains input projection, the wrapper, and persistent session management.

## Placement Rule

A concern that exists independently of parity is usually implemented in `src/`.

A concern that exists to compare against the legacy backend or to render migration artifacts is usually implemented in `app/`.

[Back to Architecture](index.md) | [Back to Documentation](../index.md)
