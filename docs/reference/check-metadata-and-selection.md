[Back to documentation index](../index.md)

# Check metadata and selection

Use this reference for the metadata fields that the catalog validates and uses
for selection.

## Definition languages

The repository supports two definition languages:

- `python`
- `dsl`

For the rationale behind that split, see
[About migrated checks](../explanation/migrated-checks.md).

## Metadata fields

- `id`: Canonical identifier for the check definition.
- `definition_language`: One of `python` or `dsl`.
- `required_context_paths`: Stable dotted paths inside
  [`CheckContext`](data-contracts.md#checkcontext) that the check
  depends on.
- `parity_baseline`: Whether the check participates in
  [strict comparison](../explanation/reference-data-and-parity.md#strict-comparison).
- `jurisdictions`: Markets where the check is eligible.
- `legacy_identity`: Derived mapping to the legacy emitted code template when
  the check participates in parity comparison.

Selection, validation, parity, and reporting all depend on this metadata.

## Selection inputs

The catalog selects checks by:

- parity baseline
- jurisdictions
- optional explicit check ids

Application [check profiles](../explanation/migrated-checks.md#check-profiles)
use these filters, then apply provider capability from the selected
[context provider](../explanation/runtime-model.md#context-providers). The public
library APIs use the same context-path capability check on the selected runtime
provider.

## Application profile extras

Application check profiles add selection behavior that is separate from the
check definition itself:

- `mode`: `all` or `include`
- `check_ids`: explicit ids for profiles with `mode = "include"`
- `migration_target_impls`
- `migration_sizes`
- `migration_risks`

The migration fields filter profiles. They only work when the application
loads a migration catalog, and they are applied after the base catalog
selection succeeds.

`migration_target_impls = ["dsl"]` selects only checks whose active migration
family is planned for `dsl`.

The reusable library APIs do not use those migration filters.

## Parity baseline

`parity_baseline` answers one question: should this check participate in parity
comparison?

- `legacy`: The check is compared against legacy behavior.
- `none`: The check runs without legacy comparison.

## Legacy identity

For checks with `parity_baseline="legacy"`, the catalog derives
`legacy_identity` from the check id. Runtime-only checks with
`parity_baseline="none"` do not have a legacy identity.

Checks do not declare a separate legacy code template. If a check participates
in parity comparison, its id is the compared legacy code template.

## Dependency invariant

For Python checks, `requires=(...)` is the explicit dependency contract. The
catalog materializes it as `required_context_paths` and validates that each path
exists in the normalized runtime contract; it does not infer dependencies from
Python source.

DSL checks do not declare this field directly. The DSL parser derives it from
the expression fields because the DSL is path based and structurally limited.

## Capability reports

Provider capability is resolved from context paths. A provider declares the
`available_context_paths` it can expose through its provider definition. The
capability resolver compares those paths with each check's
`required_context_paths` and returns runnable checks plus unsupported checks
with their missing paths.

This report does not describe source columns or derivations. That mapping
belongs to provider or source profile code, not to check metadata.

## See also

- [About migrated checks](../explanation/migrated-checks.md)
- [About the runtime model](../explanation/runtime-model.md)
- [Data contracts](data-contracts.md)

[Back to documentation index](../index.md)
