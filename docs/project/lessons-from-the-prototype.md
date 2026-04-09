[Back to documentation index](../index.md)

# Lessons learned from the prototype

This page records lessons learned while building the prototype.

## Lessons

### Parity needs an explicit comparison model

In a migration project, correctness is defined at behavioral level. A rule can
appear correct in code and still produce different results. Without an
explicit comparison model, review depends too much on personal judgment. An
explicit comparison model makes differences visible during review, gives
parity a clear meaning, and defines where exceptions belong.

In this repository, relevant references are the
[reference path](../explanation/reference-data-and-parity.md#why-the-reference-path-exists)
and
[strict comparison](../explanation/reference-data-and-parity.md#strict-comparison).
The governance layer for mismatches uses the same model.

### The DSL needs a narrow scope

A DSL is useful when a reader can understand the language quickly. Once the
language supports too many rule patterns, review becomes harder and the
boundary between rule definition and implementation becomes less clear. A
narrow DSL keeps simple checks short. More complex logic belongs in Python,
where the language already supports richer control flow and testing.

In this repository, relevant references are
[About migrated checks](../explanation/migrated-checks.md) and
[How to author checks](../how-to/author-checks.md). `dsl` is used for readable
predicates over approved fields.

### Runtime contracts need one clear owner

If the meaning of the runtime provider is spread across layers, names change and
behavior becomes harder to test. A contract needs one owner so its vocabulary
and invariants stay in one place.

In this repository, relevant references are
[data contracts](../reference/data-contracts.md) and
[CheckContext](../explanation/runtime-model.md#checkcontext).
[About the system architecture](../explanation/system-architecture.md)
describes the same boundary. The
[legacy backend](../reference/glossary.md#legacy-backend) provides reference
data through a defined interface. The runtime contract remains in Python.

### Reusable runtime behavior and application behavior need separate boundaries

A reusable library and an application run flow serve different purposes. The
library needs a compact provider and dependable contracts, while the
application needs orchestration and review support. When these concerns share
one layer, it
becomes harder to decide where a change belongs and which interface it should
preserve.

In this repository, relevant references are the split between
`src/openfoodfacts_data_quality/` and `app/`.
[About the system architecture](../explanation/system-architecture.md) and
[About application runs](../explanation/application-runs.md) describe the same
boundary.

### Tooling affects what the project can maintain

Tooling does not change rule logic directly, but it still affects how safely
the project can change. Validation and typing make regressions easier to
find, and packaging and release support make results easier to check and
reuse.

In this repository, relevant references are
[Validate changes](../how-to/validate-changes.md) and
[CI and releases](../reference/ci-and-releases.md).

### Run execution and review need one shared model

A run also needs review artifacts, stored context, and a stable place for
mismatch handling. If execution and review do not share one model, it becomes
harder to inspect runs consistently because the relevant information is spread
across files and tools. One shared model connects review to execution and
gives stored results a defined purpose.

In this repository, relevant references are
[report artifacts](../reference/report-artifacts.md) and the parity store. The
application layer also separates run execution from report building.

## Related information

- [About the project scope](../explanation/project-scope.md)
- [About the system architecture](../explanation/system-architecture.md)
- [About reference and parity](../explanation/reference-data-and-parity.md)
- [Roadmap and open questions](roadmap-and-open-questions.md)

[Back to documentation index](../index.md)
