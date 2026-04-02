# Glossary

[Documentation](index.md) / [Reference](reference/index.md) / Glossary

Canonical repository vocabulary.

Use this page as a lookup when you need exact wording. It works best once you already know the broad system shape from the project and architecture pages.

## Orientation

- `src/openfoodfacts_data_quality/` is the reusable library layer and package root for the public Python APIs.
- `app/` is the application layer that orchestrates runs, loads reference data when needed, applies strict comparison where relevant, and renders artifacts.
- the main runtime flow is:
  `source snapshot -> optional reference path -> normalized contexts -> migrated checks -> optional strict comparison -> run result -> report`

## Canonical Terms

### Check Definitions

- `definition`
  The representation of a migrated check in the repository.

- `definition language`
  The language used to express a migrated check definition.
  The canonical values are `python` and `dsl`.

- `DSL`
  The YAML based definition subsystem.
  In technical names, prefer lowercase `dsl`.

- `implementation`
  Executable runtime code.
  Use this for the migrated Python runtime, the DSL evaluator, or the legacy Perl runtime.
  Do not use it for the `python|dsl` classification axis.

### Input and Runtime

- `raw_products`
  The raw public product input surface consumed directly from the source snapshot.

- `enriched_products`
  The enriched input surface consumed from stable `EnrichedSnapshotResult` values owned by the Python runtime.
  In application runs, the reference path materializes those snapshots from the legacy backend and then projects them into the public enriched contract.

- `input surface`
  The library contract that defines which runtime surface a check supports.

- `check input surface`
  The application or config selection axis used to choose which checks are active in a run.

- `normalized context`
  The Python runtime shape consumed by migrated checks.

### Runs

- `run`
  One execution of the application against one source snapshot and one active check profile.

- `run result`
  The overall application summary for one run.
  In code, the canonical model is `RunResult`.

- `comparison status`
  The field on each check that says whether it is `compared` or `runtime_only`.

- `runtime only`
  A check execution mode with no legacy comparison baseline.
  In contracts, this corresponds to `comparison_status="runtime_only"` and `parity_baseline="none"`.

### Repository Structure

- `layer`
  Use `layer` for the major structural split of the repository, such as the shared runtime layer, the application layer, and the operational or planning layer.

- `surface`
  Use `surface` for runtime and API contracts such as `raw_products`, `enriched_products`, and the raw and enriched library APIs.

### Parity

- `reference`
  The reference side of parity comparison.
  Use `reference` for parity side runtime concepts and supporting components.

- `migrated`
  The Python output side of parity comparison.
  Use `migrated` for migrated findings and the Python side of parity comparison.
  Do not use it as the provenance label for generic snippet artifacts.

- `parity baseline`
  The selection or configuration axis that determines whether a check participates in parity comparison.
  In contracts and config, the canonical field names are `parity_baseline` and `parity_baselines`.

- `legacy identity`
  The explicit metadata that ties one migrated check compared against legacy behavior to the legacy code template used for emitted legacy tags and source provenance.
  In normalized check metadata, the canonical field name is `legacy_identity`.

- `legacy`
  The code from before migration used for snippet provenance and emitted legacy codes.
  Do not use `legacy` by itself for parity side runtime artifacts that should be called `reference`.

- `parity`
  Strict comparison between reference and migrated findings.
  Use `parity` for baselines, comparison logic, and mismatch semantics. Do not use it as the generic name for the whole application.

### Legacy Backend

- `legacy backend`
  The Perl runtime boundary used to produce internal reference side payloads.
  Use `legacy backend` for the Perl execution boundary and the code that drives it.

- `ReferenceResult`
  The explicit reference contract owned by the Python runtime and consumed by parity execution.
  In application runs, Python validates it from the versioned legacy backend result envelope before the run loop uses it.
  It contains `enriched_snapshot` and `legacy_check_tags`.

- `legacy backend result envelope`
  The internal cross language result contract emitted by the Perl wrapper.
  It carries `contract_kind`, `contract_version`, and `reference_result`.

- `legacy_check_tags`
  The raw legacy finding tags emitted by the Perl backend.

- `enriched_snapshot`
  The stable enriched product payload owned by the Python runtime and embedded in `ReferenceResult` and `EnrichedSnapshotResult`.
  In application runs, the reference path projects it from validated `ReferenceResult` values.

### Source Snapshot

- `source snapshot`
  The versioned dataset used as input for one run.
  Usually a DuckDB snapshot identified by a sidecar manifest or, when needed, by hashing the source file.

- `source_snapshot_id`
  The stable identifier of a source snapshot.
  Do not confuse this with `enriched_snapshot`.

### Report

- `origin`
  In snippet artifacts, the provenance axis for code excerpts.
  The canonical values are `legacy` and `implementation`.

- `legacy snippet status`
  The snippet artifact field on each check that says whether legacy source provenance is `available`, `not_applicable`, or `unavailable`.

- `renderer`
  A component that turns run data into HTML or report artifacts.

- `loader`
  A component that materializes runtime data from cache or an execution boundary.

### Artifacts

- `schema_version`
  The root version marker embedded in JSON artifacts such as `run.json` and `snippets.json`.

- `kind`
  The root artifact type marker embedded in JSON artifacts such as `run.json` and `snippets.json`.

### Migration Planning

- `cluster_id`
  The derived legacy inventory grouping key used in `estimation_sheet.csv` to group rows that share the same legacy source span.

## Naming Rules

- Use `layer` for the high level repository split.
- Use `run` for generic application execution and output.
- Use `reference` for parity side runtime concepts.
- Use `legacy backend` for the Perl execution boundary.
- Use `legacy` for legacy code provenance and raw legacy emitted codes.
- Use `implementation` for current repository code provenance in snippet artifacts.
- Use `migrated` for parity side Python output and mismatch semantics, not for generic snippet provenance.
- Use `legacy snippet status` for whether legacy source provenance applies or resolved for one check.
- Use `parity` only for strict comparison concepts, parity baselines, or mismatch semantics.
- Use `baseline` only for the contract or config axis.
- Use `surface` for runtime and API contracts, not for the repository structure as a whole.
- Use `dsl` in technical names such as modules, resources, paths, and scripts.
- Prefer `loader` over `provider` when the object concretely loads or materializes data.
- Prefer `renderer` over generic names like `report` when the module's responsibility is rendering output artifacts.
- Use `adapter` only for narrow interface wrappers.

## Repository Map

- `src/openfoodfacts_data_quality/`
  Reusable library contracts and the public API. The package also contains context building, the check catalog, and the DSL subsystem.

- `app/source/`
  Source snapshot access helpers.

- `app/legacy_backend/`
  The legacy backend boundary: input projection, wrapper script, persistent runner, and worker pool.

- `app/reference/`
  Reference side runtime data: models, loading logic, cache handling, envelope validation, materializers, and finding normalization.

- `app/legacy_source.py`
  Shared legacy source analysis used by report snippets and migration planning workflows.

- `app/run/`
  Run preparation and orchestration: profiles, context builders, execution, accumulation, serialization, and progress reporting.

- `app/parity/`
  Strict comparison logic between reference and migrated findings.

- `app/report/`
  Presentation: renderer, downloads, and snippets.

- `config/check-profiles.toml`
  Named run profiles and check selection policy.

## When In Doubt

- If the code refers to parity runtime state, call it `reference`.
- If the code refers to generic application execution or its overall output, call it `run`.
- If it refers to the Perl execution boundary, call it `legacy backend`.
- Prefer the package or module name already used in the repository.

## Next

- [System Overview](architecture/system-overview.md)
- [Check System](architecture/check-system.md)
- [Library Usage](guides/library-usage.md)
