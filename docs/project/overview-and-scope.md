# Project Overview and Scope

[Documentation](../index.md) / [Project](index.md) / Project Overview and Scope

The repository migrates Open Food Facts data quality checks from the legacy Perl backend into a reusable Python system.

The project also aims to:

- preserve trusted behavior where parity is expected
- leave behind a reusable check runtime, not only one migration script
- make review practical through parity artifacts and report output
- create a repeatable workflow for future migrations

## Repository Split

The repository intentionally separates three concerns:

- reusable runtime logic in `src/openfoodfacts_data_quality/`
- parity orchestration and report generation in `app/`
- migration-planning workflows in `scripts/`

That split matters because not every future caller needs the parity application, but the migration work still needs parity while the prototype is being validated.

## Current Capabilities

- a shared Python check runtime with explicit raw and enriched input surfaces
- packaged check definitions in both Python and DSL form
- a parity application that compares migrated output against the current legacy backend
- static HTML and JSON artifacts for review
- tooling to inspect legacy Perl sources and group emitted code templates into migration families

## Audiences

The current documentation and workflows are designed for three overlapping audiences:

- reviewers and mentors who need to understand the prototype and inspect its outputs
- contributors who need to run the repository and work on checks or runtime behavior
- Python callers who want to use the shared library APIs directly

## Current Status

The repository is public and usable today, but it is still a prototype.

Stable or intentionally solid parts of the current design:

- the shared runtime contracts
- the normalized context model
- the packaged check catalog
- parity-backed execution as a regular workflow
- machine-readable parity and snippet artifacts

Areas that are still intentionally in motion:

- how broad the DSL should become
- how much enriched data should be part of the long-lived public API contract
- how full-corpus runs should be operated outside small local loops
- how the report should evolve beyond migration-review needs

## Current Limits

- the repository is not yet a full replacement for every legacy data quality rule
- parity-backed execution still depends on the legacy backend
- the current report renderer is scoped to parity-compared checks
- the public Python API is explicit, but not yet presented as a fully stabilized external platform contract

## Parity Run

For a typical parity-backed run, the repository:

1. reads a DuckDB source snapshot
2. materializes reference-side enrichment and findings through the legacy backend
3. executes the selected migrated checks
4. compares reference and migrated findings under strict parity
5. emits a static report plus JSON artifacts

That workflow is already useful for review, iteration, and migration planning even though the overall project remains incomplete.

## Next Reads

- [System Overview](../architecture/system-overview.md)
- [Local Development](../guides/local-development.md)
- [Roadmap and Open Questions](roadmap-and-open-questions.md)
