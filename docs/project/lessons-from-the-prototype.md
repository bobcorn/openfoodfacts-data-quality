[Back to documentation index](../index.md)

# Lessons learned from the prototype

This page records lessons learned while building the prototype.

It sits outside the main how-to, explanation, and reference split because it
records project learning rather than product behavior.

Each section states one lesson and explains why it matters. It also points to
where that lesson is visible in the repository.

## Lessons

### Parity needs an explicit comparison model

In a migration project, correctness is defined at behavioral level. A rule can
look reasonable and still diverge in practice. An explicit comparison model
makes those differences observable and reviewable. It gives fidelity a
concrete meaning.

In this repository, that lesson is reflected in the
[reference path](../explanation/reference-data-and-parity.md#why-the-reference-path-exists)
and in
[strict comparison](../explanation/reference-data-and-parity.md#strict-comparison).
The governance layer around mismatches depends on the same principle.

### Runtime contracts need one clear owner

If the meaning of the runtime surface is spread across layers, names drift and
behavior becomes harder to test. One owner keeps the contract in one place and
makes its vocabulary easier to stabilize. In this repository, that owner is
the Python runtime.

You can see that choice in the
[data contracts](../reference/data-contracts.md) and in
[NormalizedContext](../explanation/runtime-model.md#normalizedcontext). It is
also part of the split described in
[About the system architecture](../explanation/system-architecture.md). The
[legacy backend](../reference/glossary.md#legacy-backend) still provides
reference data through a bounded interface. The runtime contract stays in
Python.

### The DSL needs a narrow scope

A DSL works when readers can hold the language in mind without much effort. As
the language grows, review gets harder and the implementation surface becomes
less clear. A narrow DSL keeps simple rules compact and leaves more structured
logic in Python, where the language already has stronger tools for expressing
it.

This boundary is visible in
[About migrated checks](../explanation/migrated-checks.md) and in
[How to author checks](../how-to/author-checks.md). In practice, `dsl` stays
close to readable predicates over approved fields.

### Reusable runtime behavior and application behavior need separate boundaries

A reusable library and an application workflow place different demands on the
code. The library needs a smaller public surface. The application needs
orchestration and review support. Keeping these concerns in one layer makes
responsibilities harder to locate and interfaces harder to preserve.

The repository expresses this lesson through the split between
`src/openfoodfacts_data_quality/` and `app/`. The same boundary is described in
[About the system architecture](../explanation/system-architecture.md) and
[About application runs](../explanation/application-runs.md).

### Run execution and review need one coherent model

The output of a run is more than evaluator results. Review needs artifacts and
stored context. It also needs a stable home for mismatch handling. When review
is part of the run model, results are easier to inspect and easier to revisit.

Here that lesson appears in the
[report artifacts](../reference/report-artifacts.md) and in the parity store.
It also appears in the separation between run execution and report building
inside the application layer.

### Tooling shapes what the project can support

Validation and typing affect the practical quality of a repository. Packaging
and release support do the same. These parts sit around the rule logic. They
still change what the project can support in practice.

That lesson is visible in
[Validate changes](../how-to/validate-changes.md) and in
[CI and releases](../reference/ci-and-releases.md).

## Related information

- [About the project scope](../explanation/project-scope.md)
- [About the system architecture](../explanation/system-architecture.md)
- [About reference data and parity](../explanation/reference-data-and-parity.md)
- [Roadmap and open questions](roadmap-and-open-questions.md)

[Back to documentation index](../index.md)
