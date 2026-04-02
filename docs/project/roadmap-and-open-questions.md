# Roadmap and Open Questions

[Back to documentation](../index.md)

Track current work and unresolved design choices here.

## Near Term Work

Current work focuses on migrating more checks with [legacy comparison](../concepts/reference-and-parity.md#strict-comparison) into the [shared runtime](../concepts/runtime-model.md#shared-runtime), tightening parity where comparison is expected, and improving the local authoring loop without weakening the explicit raw, enriched, and reference contracts.

## Open questions

### How far should the DSL grow?

The DSL stays narrow on purpose. The open question is which additions keep migrated logic obvious and reviewable.

### Which fields should enter the enriched surface?

The repository already exposes an explicit [enriched contract](../reference/data-contracts.md#enriched-snapshot) owned by the Python runtime. The open question is which additional fields deserve to widen that contract as more migrated checks land.

### Where should full corpus runs happen?

The Docker flow works for local development and modest validation loops. Parity runs over a whole snapshot may need a different home.

### How much detail should the report expose?

The report stays at summary level today. More debugging detail could help investigations. Too much detail would slow down review.

### When should the APIs become durable?

The raw and enriched APIs are explicit today. The open question is when they should be documented and supported as durable public interfaces.

## Risks to watch

### Corpus performance

A workflow that feels light on sample data can behave very differently on millions of products. Batch sizing, cache behavior, legacy backend throughput, and report usability all get more sensitive at that scale.

### Parity at scale

Low mismatch rates can still produce large mismatch volumes on full snapshots. The [artifact model](../reference/report-artifacts.md) needs to stay useful when the debugging scope grows.

### Boundary drift

The repository depends on a clean split between reusable runtime contracts, application behavior used only for parity, and support workflows. That split is present in the codebase and still needs discipline as the project grows.

[Back to documentation](../index.md)
