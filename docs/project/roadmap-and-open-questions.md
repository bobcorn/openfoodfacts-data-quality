[Back to documentation index](../index.md)

# Roadmap and open questions

This page tracks repository direction and unresolved design questions.

It is separate from the main guides, explanation pages, and reference pages
because it records project status rather than product behavior.

## Current work

Current work focuses on migrating more checks with
[strict comparison](../explanation/reference-data-and-parity.md#strict-comparison)
into the [shared runtime](../explanation/runtime-model.md#why-the-runtime-is-split).
It also improves the local authoring workflow while keeping the
[reference contracts](../reference/data-contracts.md) explicit.

## Open questions

### How far should the DSL grow?

The DSL is intentionally narrow. The open question is which additions keep
migrated logic obvious and reviewable.

### Where should whole snapshot runs happen?

The [Docker flow](../how-to/run-the-project-locally.md) supports local
development and moderate validation. Parity runs over a whole snapshot may
need a different execution environment.

### How much detail should the report expose?

The [report](../reference/report-artifacts.md#html-report) is a summary view.
More debugging detail could help investigations. Too much detail could slow
review.

### When should the APIs become durable?

The `checks` API for loaded rows is explicit. The repository also keeps a
future `snapshots` namespace reserved. The open question is when those library
surfaces should become durable public interfaces.

## Risks

### Corpus performance

A workflow that is fast on sample data can behave very differently on
millions of products. Batch sizing changes at that scale. Cache behavior and
legacy backend throughput also become more sensitive.

### Parity at scale

Low mismatch rates can still produce large mismatch volumes on full snapshots.
The [artifact model](../reference/report-artifacts.md) needs to remain useful
when the debugging scope grows.

### Boundary maintenance

The repository depends on a clean
[split](../explanation/system-architecture.md) between reusable runtime
contracts, migration behavior used only for parity, and support workflows.
That split is present in the codebase and still needs careful maintenance as
the project grows.

## Related information

- [Lessons learned from the prototype](lessons-from-the-prototype.md)
- [About the project scope](../explanation/project-scope.md)
- [About reference and parity](../explanation/reference-data-and-parity.md)
- [Report artifacts](../reference/report-artifacts.md)

[Back to documentation index](../index.md)
