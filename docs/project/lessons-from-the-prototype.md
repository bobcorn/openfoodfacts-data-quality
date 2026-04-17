[Back to documentation index](../index.md)

# Lessons learned from the prototype

This page records the design lessons that shaped the current repository. It
focuses on decisions that changed module boundaries, data contracts, and the
review workflow.

### A migration needs a reference model

Early work treated the migration as a code port. That view broke down once
parity depended on emitted backend envelopes, normalized findings, cached
reference data, and strict comparison rules. The repository now models those
concerns explicitly through
[`ReferenceResult`](../reference/data-contracts.md#referenceresult),
[`ObservedFinding`](../reference/data-contracts.md#observedfinding),
[reference loading](../explanation/reference-data-and-parity.md), and the
[migration run flow](../explanation/migration-runs.md).

Translated code does not preserve behavior on its own. A migration needs a
stable description of old output, normalization rules, and comparison rules.
Once those contracts exist, mismatch review becomes repeatable and changes can
be discussed with shared evidence.

### Normalize input at the boundary

The shared runtime now works on one canonical source contract,
[`SourceProduct`](../reference/data-contracts.md#sourceproduct). Adapters
absorb the differences between OFF exports, JSONL documents, Parquet rows,
DuckDB relations, and sparse canonical rows. The migration path keeps its own
[`ProductDocument`](../reference/data-contracts.md#productdocument) because the
legacy backend has different needs.

That split kept input variability out of the core runtime. Checks and context
builders can assume one shape, while unsupported partial inputs fail at ingest.
Normalization belongs at the edge of the system, where format knowledge already
exists.

### Reusable runtime and migration tooling need separate ownership

As the prototype grew, the repository stopped acting like one application. The
code under `src/` needed a small public API and stable contracts. The code
under `migration/` needed freedom to change while the parity workflow was still
being refined. Local apps in `apps/` benefited from consuming the packaged
runtime instead of reaching into migration internals. [About the system
architecture](../explanation/system-architecture.md) and [About the project
scope](../explanation/project-scope.md) now reflect that split.

Shared code does not imply shared ownership. Different consumers bring
different stability expectations, and package boundaries should follow them
once those expectations are visible.

### Review workflows deserve a real data model

Parity work started with transient execution output. That was enough for
debugging single runs, but it did not support ongoing review. The repository
later added dataset profiles, stored runs, mismatch rows, snapshots, and
report artifacts around [`RunResult`](../reference/data-contracts.md#runresult).
[Report artifacts](../reference/report-artifacts.md) and [run configuration
and artifacts](../reference/run-configuration-and-artifacts.md) describe that
model.

Review becomes part of the system when people use results to decide whether a
migration is acceptable. Logs are useful during development, but they are a
weak foundation for audit and comparison. Durable artifacts and stored state
make the review loop easier to repeat and easier to trust.

### Keep extension points narrow

The repository now relies on explicit check metadata, context providers,
capability checks, check profiles, and a small DSL. More complex logic still
goes in Python. That division is described in
[About migrated checks](../explanation/migrated-checks.md), [check metadata
and selection](../reference/check-metadata-and-selection.md), and [How to
author checks](../how-to/author-checks.md).

Extensibility depends on clear rules more than on generic abstractions. A
narrow declarative path is easier to review and maintain. A separate
imperative path gives the project room for exceptions without forcing the DSL
to absorb every case.

### Prototype scaffolding should be easy to remove

Several intermediate layers were useful during early exploration and later
became noise. The repository improved when temporary planning layers, older
public surfaces, and migration rules that no longer matched the execution model
were removed. [About the system architecture](../explanation/system-architecture.md),
[About migration runs](../explanation/migration-runs.md), and the
[roadmap](roadmap-and-open-questions.md) reflect the current structure more
directly.

A prototype earns its keep by exposing the problem. Some of its scaffolding
should disappear once the real boundaries are clear, because obsolete layers
kept for historical continuity usually make the codebase harder to explain and
harder to change.

## Related information

- [About the project scope](../explanation/project-scope.md)
- [About the system architecture](../explanation/system-architecture.md)
- [About reference and parity](../explanation/reference-data-and-parity.md)
- [Roadmap and open questions](roadmap-and-open-questions.md)

[Back to documentation index](../index.md)
