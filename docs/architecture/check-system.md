# Check System

[Documentation](../index.md) / [Architecture](index.md) / Check System

Checks are packaged definitions loaded through one catalog and executed through one runtime, whether their logic is written in Python or in the DSL.

## Definition Languages

The check system supports two definition languages:

- `python`
  For checks that need loops, helper-driven logic, aggregation, dynamic emitted codes, or other imperative behavior.
- `dsl`
  For readable boolean predicates over approved normalized-context paths.

Both languages share the same catalog, selection model, and metadata concepts.

## Check Definition Metadata

Every check definition has stable metadata:

- canonical check id
- definition language
- required normalized-context paths
- supported input surfaces
- parity baseline
- jurisdictions
- optional legacy identity

Most execution behavior is selected from this metadata, not from ad hoc application conditionals.

## Selection Model

Checks are selected through the catalog by:

- input surface
- parity baseline
- jurisdictions
- optional explicit check ids

The same selection model is used by the public library APIs and by application run profiles.

## Parity Baseline

`parity_baseline` answers one specific question: should this check participate in parity comparison?

- `legacy`
  The check is legacy-backed and participates in parity.
- `none`
  The check is runtime-only and can execute without legacy comparison.

This is a more useful axis than informal labels such as "old" and "new".

## Legacy Identity

Parity-backed checks may need an explicit mapping to the legacy emitted code template. That mapping is stored as `legacy_identity`.

This allows the application to compare one migrated check against the correct legacy-side tags even when the migrated implementation no longer mirrors the Perl source structure.

## DSL Scope

The DSL is intentionally small. It is meant for checks that are fundamentally:

- a boolean predicate
- over stable normalized-context fields
- with one static severity
- without helper-driven control flow

It does not model:

- iteration
- aggregation
- cross-field arithmetic
- field-to-field comparison
- helper calls
- dynamic emitted codes
- stateful logic

Those cases stay in Python.

## Runtime Invariant

For Python checks, declared `requires=(...)` metadata is validated against inferred context usage from the check function and helper annotations.

The contract is enforced, not just documented.

## Next Reads

- [Authoring Checks](../guides/authoring-checks.md)
- [Data Contracts](data-contracts.md)
- [Glossary](../glossary.md)
