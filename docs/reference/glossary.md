[Back to documentation index](../index.md)

# Glossary

Use this glossary when you need the canonical repository term for a runtime
concept, contract, or boundary.

For execution details, see
[About the runtime model](../explanation/runtime-model.md),
[About migrated checks](../explanation/migrated-checks.md), and
[About reference and parity](../explanation/reference-data-and-parity.md).

## Canonical terms

### Check definitions

- `definition`
  repository representation of a migrated check
- `definition language`
  language used to express one migrated check; canonical values are `python`
  and `dsl`
- `DSL`
  definition subsystem written in YAML and used as one of the check definition
  languages; use `DSL` in prose and `dsl` in technical names or literal values
- `implementation`
  executable runtime code; use this for the migrated Python runtime, the DSL
  evaluator, or the legacy Perl runtime, not for the `python|dsl`
  classification axis
- `jurisdiction`
  market scope attached to a check definition; in metadata, `jurisdictions`
  limits where a check is eligible

### Input and runtime

- `checks`
  public library namespace for checks on loaded rows
- `off_data_quality`
  public import namespace for library callers
- `snapshots`
  reserved public namespace for a future enrichment-focused library API
- `openfoodfacts_data_quality`
  implementation package root for the shared runtime and contracts
- `source_products`
  context provider consumed directly from source products
- `enriched_snapshots`
  enriched snapshot context provider consumed from stable enriched data; in the
  app reference path this arrives as `CheckContext`, while direct
  library usage accepts `EnrichedSnapshotRecord`
- `context provider`
  current library bridge that defines which check context paths a provider
  can expose
- `check context provider`
  application or config selection axis used to choose the context provider for
  one run
- `check context`
  Python runtime contract used by migrated checks
- `dataset profile`
  named application preset that selects which products from the source snapshot
  enter one run
- `source selection`
  explicit selection contract resolved from one dataset profile

### Runs

- `run`
  one execution of the application against one source snapshot, one dataset
  profile, and one active check profile
- `run spec`
  explicit application configuration resolved before orchestration starts; in
  code, the canonical model is `RunSpec`
- `run result`
  overall application summary for one run; in code, the canonical model is
  `RunResult`
- `check profile`
  named run preset loaded from `config/check-profiles.toml`
- `comparison status`
  field on each check that says whether it is `compared` or `runtime_only`
- `runtime only`
  check execution mode with no legacy comparison baseline; in contracts, this
  corresponds to `comparison_status="runtime_only"` and
  `parity_baseline="none"`

### Repository structure

- `layer`
  major structural split of the repository, such as the shared runtime layer or
  the application layer
- `shared runtime`
  reusable execution layer under `src/openfoodfacts_data_quality/`

### Parity

- `reference`
  parity runtime data and support components
- `migrated`
  Python output side of parity comparison
- `parity baseline`
  metadata axis that decides whether a check participates in parity comparison
- `legacy identity`
  metadata that ties one migrated check to the legacy code template used for
  emitted tags and source provenance
- `legacy`
  code from before migration used for snippet provenance and emitted legacy
  codes; do not use it as the generic name for parity runtime artifacts
- `parity`
  strict comparison between reference and migrated findings

### Migration planning

- `migration catalog`
  application-owned view of legacy families plus optional planning metadata
- `migration family`
  one legacy emission family joined with optional assessment fields such as
  target implementation, size, and risk
- `active migration plan`
  migration family coverage for the active checks in one run

### Legacy backend

- `legacy backend`
  Perl runtime boundary used to produce payloads for the reference side
- `ReferenceResult`
  reference contract owned by the Python runtime and consumed by parity
  execution
- `legacy backend result envelope`
  internal result contract emitted across the language boundary by the Perl
  wrapper
- `legacy_check_tags`
  raw legacy finding tags emitted by the Perl backend
- `enriched_snapshot`
  stable enriched product payload embedded in `ReferenceResult` and
  `EnrichedSnapshotRecord`

### Source snapshot

- `source snapshot`
  versioned full product dataset used as input for one application run. The
  current run application supports JSONL snapshots and DuckDB snapshots with a
  `products` table.
- `source_snapshot_id`
  stable identifier of a source snapshot

### Report and storage

- `parity store`
  DuckDB store kept by the application for run telemetry, mismatches,
  governance summaries, dataset metadata, migration metadata, and a serialized
  run artifact
- `recorded run snapshot`
  read model used by the report renderer for data loaded from the parity store
- `origin`
  provenance axis for snippet artifacts; canonical values are `legacy` and
  `implementation`
- `legacy snippet status`
  snippet artifact field that says whether legacy source provenance is
  `available`, `not_applicable`, or `unavailable`
- `renderer`
  component that turns run data into HTML or report artifacts
- `loader`
  component that materializes runtime data from cache or an execution boundary

### Artifacts

- `schema_version`
  root version marker embedded in JSON artifacts such as `run.json` and
  `snippets.json`
- `kind`
  root artifact type marker embedded in JSON artifacts such as `run.json` and
  `snippets.json`
- `review artifacts`
  generated outputs used to inspect one run, especially the HTML report,
  `run.json`, and `snippets.json`

## Naming preferences

- Use `reference` for parity runtime data and support components.
- Use `legacy backend` for the Perl execution boundary.
- Use `legacy` for code provenance or raw legacy emitted codes.
- Use `migrated` for Python output and mismatch semantics.
- Use `implementation` for repository code provenance in snippet
  artifacts.
- Use `parity` for strict comparison concepts and mismatch semantics.
- Prefer `DSL` in prose and `dsl` in technical names or literal values.

[Back to documentation index](../index.md)
