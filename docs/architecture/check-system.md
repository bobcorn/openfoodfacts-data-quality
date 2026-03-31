# Check System

[Documentation](../index.md) / [Architecture](index.md) / Check System

Checks are packaged definitions loaded through one catalog.

## Definition Languages

The repository supports two definition languages:

- `python` for checks whose logic is easier to express imperatively
- `dsl` for simple predicate-style checks over normalized fields

Both languages share the same catalog, metadata model, and execution flow.

## DSL Scope

The DSL covers checks that are fundamentally:

- a boolean predicate
- over stable normalized-context fields
- with one static severity
- without helper-driven control flow

Use Python for checks that need richer logic.

## What A Check Definition Carries

Every check definition has stable metadata:

- canonical check id
- definition language
- required normalized-context paths
- supported input surfaces
- parity baseline
- jurisdictions
- optional legacy identity

Most runtime behavior comes from these metadata fields.

## Selection Model

Checks are selected through the catalog by:

- input surface
- parity baseline
- jurisdictions
- optional explicit check ids

The same selection model is used by the public library surface and by application profiles.

## Parity Baseline

`parity_baseline` answers one question: should this check participate in parity comparison?

- `legacy` means the check is parity-backed
- `none` means the check can run in the migrated runtime without legacy comparison

This matters more than informal labels such as "old" or "new".

## Legacy Identity

Parity-backed checks may need an explicit mapping to the legacy emitted code template. That mapping is stored as `legacy_identity`.

This lets the application compare one migrated check against the correct legacy-side tags, even when the runtime code is no longer structured like the Perl source.

## DSL Limits

The DSL is intentionally narrow. It is meant for readable boolean predicates over approved normalized-context paths.

It does not model:

- iteration
- aggregation
- cross-field arithmetic
- field-to-field comparison
- helper calls
- dynamic emitted codes
- stateful logic

Those cases stay in Python.

[Back to Architecture](index.md) | [Back to Documentation](../index.md)
