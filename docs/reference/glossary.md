# Glossary

[Back to documentation](../index.md)

Look here for the canonical repository term for a runtime concept, contract, or boundary.

For execution details, see [How the Runtime Model Works](../concepts/runtime-model.md), [What a Migrated Check Contains](../concepts/check-model.md), and [Why Compared Runs Load Reference Data](../concepts/reference-and-parity.md).

## Orientation

- `src/openfoodfacts_data_quality/` is the reusable library layer and package root for the public Python APIs.
- `app/` is the application layer for source loading, reference loading, strict comparison, and report output.
- The main runtime flow is `source snapshot -> optional reference path -> normalized context -> migrated checks -> optional strict comparison -> run result -> report`.

## Canonical Terms

### Check Definitions

- `definition`
  repository representation of a migrated check
- `definition language`
  language used to express one migrated check; canonical values are `python` and `dsl`
- `DSL`
  definition subsystem written in YAML and used as one of the check definition languages; use `DSL` in prose and `dsl` in technical names or literal values
- `implementation`
  executable runtime code; use this for the migrated Python runtime, the DSL evaluator, or the legacy Perl runtime, not for the `python|dsl` classification axis
- `jurisdiction`
  market scope attached to a check definition; in metadata, `jurisdictions` limits where a check is eligible

### Input and Runtime

- `raw_products`
  raw public product input surface consumed directly from the source snapshot
- `enriched_products`
  enriched input surface consumed from stable `EnrichedSnapshotResult` values owned by the Python runtime
- `input surface`
  library contract that defines which runtime surface a check supports
- `check input surface`
  application or config selection axis used to choose which checks are active in a run
- `normalized context`
  Python runtime shape consumed by migrated checks

### Runs

- `run`
  one execution of the application against one source snapshot and one active check profile
- `run result`
  overall application summary for one run; in code, the canonical model is `RunResult`
- `check profile`
  named run preset loaded from `config/check-profiles.toml`
- `comparison status`
  field on each check that says whether it is `compared` or `runtime_only`
- `runtime only`
  check execution mode with no legacy comparison baseline; in contracts, this corresponds to `comparison_status="runtime_only"` and `parity_baseline="none"`

### Repository Structure

- `layer`
  major structural split of the repository, such as the shared runtime layer or the application layer
- `shared runtime`
  reusable execution layer under `src/openfoodfacts_data_quality/`
- `surface`
  runtime and API contract such as `raw_products`, `enriched_products`, and the raw or enriched library APIs

### Parity

- `reference`
  parity runtime data and support components
- `migrated`
  Python output side of parity comparison
- `parity baseline`
  metadata axis that decides whether a check participates in parity comparison
- `legacy identity`
  metadata that ties one migrated check to the legacy code template used for emitted tags and source provenance
- `legacy`
  code from before migration used for snippet provenance and emitted legacy codes; do not use it as the generic name for parity runtime artifacts
- `parity`
  strict comparison between reference and migrated findings

### Legacy Backend

- `legacy backend`
  Perl runtime boundary used to produce payloads for the reference side
- `ReferenceResult`
  reference contract owned by the Python runtime and consumed by parity execution
- `legacy backend result envelope`
  internal result contract emitted across the language boundary by the Perl wrapper
- `legacy_check_tags`
  raw legacy finding tags emitted by the Perl backend
- `enriched_snapshot`
  stable enriched product payload embedded in `ReferenceResult` and `EnrichedSnapshotResult`

### Source Snapshot

- `source snapshot`
  versioned dataset used as input for one run, usually a DuckDB snapshot identified by a sidecar manifest or a file hash
- `source_snapshot_id`
  stable identifier of a source snapshot

### Report

- `origin`
  provenance axis for snippet artifacts; canonical values are `legacy` and `implementation`
- `legacy snippet status`
  snippet artifact field that says whether legacy source provenance is `available`, `not_applicable`, or `unavailable`
- `renderer`
  component that turns run data into HTML or report artifacts
- `loader`
  component that materializes runtime data from cache or an execution boundary

### Artifacts

- `schema_version`
  root version marker embedded in JSON artifacts such as `run.json` and `snippets.json`
- `kind`
  root artifact type marker embedded in JSON artifacts such as `run.json` and `snippets.json`
- `review artifacts`
  generated outputs used to inspect one run, especially the HTML report, `run.json`, and `snippets.json`

## Naming Rules

- Use `layer` for the high level repository split.
- Use `run` for generic application execution and output.
- Use `reference` for parity runtime concepts.
- Use `legacy backend` for the Perl execution boundary.
- Use `legacy` for legacy code provenance and raw legacy emitted codes.
- Use `implementation` for current repository code provenance in snippet artifacts.
- Use `migrated` for Python output and mismatch semantics, not for generic snippet provenance.
- Use `legacy snippet status` for the state of legacy source provenance on one check.
- Use `parity` only for strict comparison concepts, parity baselines, or mismatch semantics.
- Use `surface` for runtime and API contracts, not for the repository structure as a whole.
- Prefer `DSL` in prose and `dsl` in technical names such as modules, resources, paths, scripts, and literal values.
- Prefer `loader` over `provider` when the object concretely loads or materializes data.
- Prefer `renderer` over generic names like `report` when the module's responsibility is rendering output artifacts.

## Repository Map

- `src/openfoodfacts_data_quality/`
  reusable library contracts and the public API; the package also contains context building, the check catalog, and the DSL subsystem
- `app/source/`
  source snapshot access helpers
- `app/legacy_backend/`
  legacy backend boundary, input projection, wrapper script, persistent runner, and worker pool
- `app/reference/`
  runtime data for the reference side, loading logic, cache handling, envelope validation, materializers, and finding normalization
- `app/legacy_source.py`
  shared legacy source analysis used by report snippets
- `app/run/`
  run preparation, orchestration, profiles, context builders, execution, accumulation, serialization, and progress reporting
- `app/parity/`
  strict comparison logic between reference and migrated findings
- `app/report/`
  renderer, downloads, and snippets
- `config/check-profiles.toml`
  named run profiles and check selection policy

## Default Choices

- If the code refers to parity runtime state, call it `reference`.
- If the code refers to generic application execution or its overall output, call it `run`.
- If the code refers to the Perl execution boundary, call it `legacy backend`.
- Prefer the package or module name already used in the repository.

[Back to documentation](../index.md)
