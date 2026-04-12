[Back to documentation index](../index.md)

# About the project scope

This repository migrates legacy data quality checks into a reusable Python
system with parity validation against the legacy backend.

The goal is behavioral fidelity first, then a runtime that is easier to test,
run locally, and evolve.

## Why the repository is split

The repository keeps reusable runtime logic in
`src/off_data_quality/` and migration orchestration in `migration/`.

Python callers can use the shared runtime without the migration tooling.
Strict parity runs still depend on the legacy backend through the
[reference path](reference-data-and-parity.md#why-the-reference-path-exists).
Live backend execution happens only on cache misses.

For the repository map and the architecture overview diagram, see
[About the system architecture](system-architecture.md).

## What the repository already supports

- A shared Python runtime with explicit source product and enriched snapshot context providers.
- Runtime contracts owned by Python for source products, enriched snapshots,
  reference results, and check context.
- Packaged checks written in Python and `dsl`.
- Application runs that mix strict-parity checks with checks that run without
  comparison.
- Runs over whole snapshots or deterministic subsets.
- Static HTML output plus JSON artifacts for review.

## What is stable enough to build on

- the shared [runtime contracts](../reference/data-contracts.md)
- the [`CheckContext`](runtime-model.md#checkcontext) model
- the packaged [check catalog](../reference/check-metadata-and-selection.md)
- [migration runs](migration-runs.md) as a regular workflow
- [run and snippet artifacts](../reference/report-artifacts.md)
- review data from one completed run in the parity store

## What is still evolving

- how broad the DSL should become
- where whole snapshot runs should live outside short local loops
- how the report should evolve beyond migration review
- when the `checks` API and the future `snapshots` API should become durable
  public interfaces

## Limits

- The repository is not yet a full replacement for every legacy data-quality
  rule.
- Strict parity runs still depend on the
  [ReferenceResult](../reference/data-contracts.md#referenceresult) contract
  and the
  [reference path](reference-data-and-parity.md#why-the-reference-path-exists)
  behind it.
- The report is optimized for review, not exhaustive debugging detail.
- The public Python APIs are explicit project contracts, but they are not yet
  durable compatibility promises.

## Related information

- [About the system architecture](system-architecture.md)
- [About migration runs](migration-runs.md)
- [Roadmap and open questions](../project/roadmap-and-open-questions.md)

[Back to documentation index](../index.md)
