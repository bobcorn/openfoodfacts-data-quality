# Project Overview and Scope

[Documentation](../index.md) / [Project](index.md) / Project Overview and Scope

The repository migrates Open Food Facts data quality checks from the [legacy Perl backend](../architecture/application-run-flow.md#legacy-backend) into a reusable Python system.

It aims to preserve trusted behavior where [strict comparison](../architecture/application-run-flow.md#strict-comparison) is expected. It provides a Python runtime for migrated checks through the [shared runtime](../architecture/system-overview.md#shared-runtime) and supports [runtime only checks](../architecture/check-system.md#parity-baseline) without forcing a parity path. It also records a [migration workflow](../guides/authoring-checks.md#workflow) and produces [JSON artifacts](../operations/configuration-and-artifacts.md) and [report output](../getting-started/reading-the-report.md) for review.

## Repository Split

The repository separates three concerns:

- reusable runtime logic in `src/openfoodfacts_data_quality/`
- application orchestration, comparison, and report generation in `app/`
- migration planning workflows in `scripts/`

This split is described in more detail in the [System Overview](../architecture/system-overview.md). Python callers can use the shared runtime without the [application run layer](../architecture/application-run-flow.md). Compared runs and enriched application runs still depend on the [legacy backend](../architecture/application-run-flow.md#legacy-backend) through the [reference path](../architecture/application-run-flow.md#reference-path). Live backend execution happens only on cache misses.

## Capabilities

- a shared Python check runtime with explicit [raw and enriched input surfaces](../architecture/data-contracts.md#input-surfaces)
- [raw, enriched, and normalized runtime contracts](../architecture/data-contracts.md) owned by the Python runtime
- packaged [check definitions in Python and dsl](../architecture/check-system.md)
- an [application run layer](../architecture/application-run-flow.md) that supports compared and runtime only checks
- static HTML and [JSON run artifacts](../operations/configuration-and-artifacts.md) for review
- tooling to inspect legacy Perl sources and group emitted code templates into [migration families](../operations/legacy-inventory.md)

## Status

The repository is in a prototype phase.

More stable parts:

- the shared [runtime contracts](../architecture/data-contracts.md)
- the explicit [raw and enriched input contracts](../architecture/data-contracts.md)
- the [normalized context](../architecture/data-contracts.md#normalizedcontext) model
- the packaged [check catalog](../architecture/check-system.md)
- [application execution](../architecture/application-run-flow.md) as a regular workflow
- [JSON run and snippet artifacts](../operations/configuration-and-artifacts.md)

Less settled parts:

- how broad the [dsl](../architecture/check-system.md#dsl-scope) should become
- how full corpus runs should be operated outside small local loops
- how the [report](../getting-started/reading-the-report.md) should evolve beyond migration review needs

## Limits

- the repository is not yet a full replacement for every legacy data quality rule
- compared runs and enriched application runs still depend on the [legacy backend contract](../architecture/data-contracts.md#referenceresult) through the [reference path](../architecture/application-run-flow.md#reference-path)
- the [report](../getting-started/reading-the-report.md) still optimizes for review rather than exhaustive debugging detail
- the public Python APIs are explicit [project contracts](../architecture/data-contracts.md), but not yet long term compatibility promises

## Run Flow

For a typical application run, the repository:

1. reads a DuckDB [source snapshot](../glossary.md)
2. resolves reference enrichment and findings through the [reference path](../architecture/application-run-flow.md#reference-path), which checks the cache first and falls back to the [legacy backend](../architecture/application-run-flow.md#legacy-backend) on cache misses when the selected checks need them
3. executes the selected migrated checks
4. compares reference and migrated findings under [strict comparison](../architecture/application-run-flow.md#strict-comparison)
5. emits a static [report](../getting-started/reading-the-report.md) plus [JSON artifacts](../operations/configuration-and-artifacts.md)

This workflow supports review and migration planning.

## Next

- [System Overview](../architecture/system-overview.md)
- [Local Development](../guides/local-development.md)
- [Roadmap and Open Questions](roadmap-and-open-questions.md)
