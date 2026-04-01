# Project Overview and Scope

[Documentation](../index.md) / [Project](index.md) / Project Overview and Scope

The repository migrates Open Food Facts data quality checks from the legacy Perl backend into a reusable Python system.

The project also aims to:

- preserve trusted behavior where parity is expected
- provide a reusable check runtime after the migration
- make review practical through JSON artifacts and report output
- support runtime only checks without forcing a parity path
- create a repeatable workflow for future migrations

## Repository Split

The repository separates three concerns:

- reusable runtime logic in `src/openfoodfacts_data_quality/`
- application orchestration, comparison, and report generation in `app/`
- migration planning workflows in `scripts/`

Python callers can use the shared runtime without the application run layer. Compared runs and enriched application runs still depend on the legacy backend through the reference path. Live backend execution happens only on cache misses.

## Current Capabilities

- a shared Python check runtime with explicit raw and enriched input surfaces
- raw, enriched, and normalized runtime contracts owned by the Python runtime
- packaged check definitions in Python and DSL
- an application run layer that supports compared and runtime only checks
- static HTML and JSON run artifacts for review
- tooling to inspect legacy Perl sources and group emitted code templates into migration families

## Audiences

The documentation and workflows serve three overlapping audiences:

- reviewers and mentors who need to understand the prototype and inspect its outputs
- contributors who need to run the repository and work on checks or runtime behavior
- Python callers who want to use the shared library APIs directly

## Current Status

The repository is in a prototype phase.

More stable parts:

- the shared runtime contracts
- the explicit raw and enriched public input contracts
- the normalized context model
- the packaged check catalog
- application execution as a regular workflow
- JSON run and snippet artifacts

Less settled parts:

- how broad the DSL should become
- how full corpus runs should be operated outside small local loops
- how the report should evolve beyond migration review needs

## Current Limits

- the repository is not yet a full replacement for every legacy data quality rule
- compared runs and enriched application runs still depend on the legacy backend contract through the reference path
- the report still optimizes for review rather than exhaustive debugging detail
- the public Python APIs are explicit project contracts, but not yet documented as broad community-facing compatibility promises

## Application Run

For a typical application run, the repository:

1. reads a DuckDB source snapshot
2. resolves reference enrichment and findings through a reference path that checks the cache first and falls back to the legacy backend on cache misses when the selected checks need them
3. executes the selected migrated checks
4. compares reference and migrated findings under strict comparison where a legacy baseline exists
5. emits a static report plus JSON artifacts

This workflow supports review, iteration, and migration planning.

## Next Reads

- [System Overview](../architecture/system-overview.md)
- [Local Development](../guides/local-development.md)
- [Roadmap and Open Questions](roadmap-and-open-questions.md)
