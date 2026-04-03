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
- `supported_input_surfaces`: The
  [input surfaces](../explanation/runtime-model.md#input-surfaces) that can
  execute the check.
- `required_context_paths`: Stable dotted paths inside
  [`NormalizedContext`](data-contracts.md#normalizedcontext) that the check
  depends on.
- `parity_baseline`: Whether the check participates in
  [strict comparison](../explanation/reference-data-and-parity.md#strict-comparison).
- `jurisdictions`: Markets where the check is eligible.
- `legacy_identity`: Explicit mapping to the correct legacy emitted code
  template when the default mapping is not enough.

Selection, validation, parity, and reporting all depend on this metadata.

## Selection inputs

The catalog selects checks by:

- input surface
- parity baseline
- jurisdictions
- optional explicit check ids

Application [check profiles](../explanation/migrated-checks.md#check-profiles)
use all of these filters. The public library APIs use the same
[input surface](../explanation/runtime-model.md#input-surfaces), jurisdiction,
and explicit id filters on the selected runtime surface.

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

Checks compared against legacy behavior may need an explicit mapping to the
legacy emitted code template. That mapping is stored as `legacy_identity`.

This mapping points one migrated check to the correct legacy tags even when the
migrated implementation no longer mirrors the Perl source structure.

For most checks with `parity_baseline="legacy"`, the catalog derives this
mapping from the check id. Use `legacy_identity` when the compared legacy code
template has a
different canonical name. A check can keep id `en:contract-test` while mapping
parity to legacy code template
`en:legacy-contract-test`.

## Dependency invariant

For Python checks, declared `requires=(...)` metadata is validated against
inferred context usage from the check function and helper annotations.

Helpers that receive `context` or any whole or intermediate context object must
declare their dependency paths explicitly with `@depends_on_context_paths(...)`.
See [Author checks](../how-to/author-checks.md#extend-contracts-on-purpose).

## See also

- [About migrated checks](../explanation/migrated-checks.md)
- [About the runtime model](../explanation/runtime-model.md)
- [Data contracts](data-contracts.md)

[Back to documentation index](../index.md)
