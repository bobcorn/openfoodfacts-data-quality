# Glossary

[Documentation](index.md) / [Reference](reference/index.md) / Glossary

This document defines the repository vocabulary.

Use one preferred term for each concept.
If a word is ambiguous, prefer the term defined here over a synonym or shorthand.

## Read This First

- `src/openfoodfacts_data_quality/` is the reusable library surface.
- `app/` is the application layer that orchestrates runs, parity, and reporting.
- The runtime flow is:
  `source snapshot -> legacy backend boundary -> reference-side runtime data -> migrated checks -> parity -> report`.

## Canonical Terms

### Check Definition Terms

- `definition`
  The representation of a migrated check in the repository.

- `definition language`
  The language used to express a migrated check definition.
  The canonical values are `python` and `dsl`.

- `DSL`
  The YAML-backed definition subsystem.
  In technical names, prefer lowercase `dsl`.

- `implementation`
  Executable runtime code.
  Use this for the migrated Python runtime, the DSL evaluator, or the legacy Perl runtime.
  Do not use it for the `python|dsl` classification axis.

### Input And Runtime Terms

- `raw_products`
  The raw public product input surface consumed directly from the source snapshot.

- `enriched_products`
  The enriched input surface available after the legacy backend has produced an `enriched_snapshot`.

- `input surface`
  The library-level contract that defines which runtime surface a check supports.

- `check input surface`
  The application or config selection axis used to choose which checks are active in a run.

- `normalized context`
  The Python-owned runtime shape consumed by migrated checks.

### Parity Terms

- `reference`
  The reference side of parity comparison.
  Use `reference` for parity-side runtime concepts and supporting components.

- `migrated`
  The Python-owned output side of parity comparison.
  Use `migrated` for migrated findings, migrated snippets, and migrated implementation code.

- `parity baseline`
  The selection or configuration axis that determines whether a check participates in parity comparison.
  In contracts and config, the canonical field names are `parity_baseline` and `parity_baselines`.

- `legacy identity`
  The explicit metadata that ties one parity-backed migrated check to the legacy code template used for emitted legacy tags and source provenance.
  In normalized check metadata, the canonical field name is `legacy_identity`.

- `legacy`
  The pre-migration code side used for snippet provenance and emitted legacy codes.
  Do not use `legacy` by itself for parity-side runtime artifacts that should be called `reference`.

### Legacy Backend Terms

- `legacy backend`
  The Perl runtime boundary used to produce the reference-side payloads.
  Use `legacy backend` for the Perl execution boundary and the code that drives it.

- `ReferenceResult`
  The explicit runtime contract produced by the legacy backend boundary and consumed by parity execution.
  It contains `enriched_snapshot` and `legacy_check_tags`.

- `legacy_check_tags`
  The raw legacy finding tags emitted by the Perl backend.

- `enriched_snapshot`
  The backend-owned enriched product payload embedded in `ReferenceResult`.

### Source Snapshot Terms

- `source snapshot`
  The versioned dataset used as input for one run.
  In practice, this is usually a DuckDB snapshot identified by hashing the source file.

- `source_snapshot_id`
  The stable identifier of a source snapshot.
  Do not confuse this with `enriched_snapshot`.

### Report Terms

- `origin`
  In snippet artifacts, the provenance axis for code excerpts.
  The canonical values are `legacy` and `migrated`.

- `renderer`
  A component that turns parity-domain data into HTML or report artifacts.

- `loader`
  A component that materializes runtime data from cache or an execution boundary.

## Naming Rules

- Use `reference` for parity-side runtime concepts:
  `ReferenceResult` and reference-side findings or support components.

- Use `legacy backend` for the Perl boundary:
  worker pools, input projection, and wrapper scripts.
  Use it for similar runtime details as well.

- Use `legacy` for legacy code provenance and raw legacy emitted codes, not for parity-side runtime payloads.

- Use `migrated` for Python-owned output and snippet provenance on the migrated side.

- Use `baseline` only for the contract or config axis:
  `parity_baseline`, `parity_baselines`.
  Do not use it for runtime class or module names when `reference` is the real concept.

- Use `dsl` in technical names such as modules, resource helpers, pack paths, and scripts.

- Prefer `loader` over `provider` when the object concretely loads or materializes data.

- Prefer `renderer` over generic names like `report` when the module's responsibility is rendering output artifacts.

- Use `adapter` only for narrow interface wrappers.

## Repository Map

- `src/openfoodfacts_data_quality/`
  Reusable library contracts and the public API live here. The package also contains context building, the check catalog, and the DSL subsystem.

- `app/sources/`
  Source snapshot access helpers.

- `app/legacy_backend/`
  The legacy backend boundary:
  input projection and the wrapper script.
  The persistent runner and the worker pool also live here.

- `app/reference/`
  Reference-side runtime data:
  models and loading logic.
  Cache handling and finding normalization also live here.

- `app/pipeline/`
  Run preparation and orchestration:
  profiles, context builders, and batch input resolution.
  The execution loop and progress reporting also live here.

- `app/parity/`
  Parity-domain models plus comparison and accumulation logic. Artifact serialization also lives here.

- `app/report/`
  Presentation:
  renderer and preview server.
  Downloads, snippets, and templates also live here.

- `config/check-profiles.toml`
  Named run profiles and check-selection policy.

## When In Doubt

- If the code refers to parity runtime state, call it `reference`.
- If it refers to the Perl execution boundary, call it `legacy backend`.
- Prefer the package or module name already used in the repository.

[Back to Reference](reference/index.md) | [Back to Documentation](index.md)
