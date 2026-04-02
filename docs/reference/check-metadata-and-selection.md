# Check Metadata and Selection

[Back to documentation](../index.md)

The catalog validates and selects checks from these executable metadata fields.

## Definition languages

The repository supports two definition languages:

- `python`
- `dsl`

The rationale for that split lives in [What a Migrated Check Contains](../concepts/check-model.md).

## Metadata fields

- `id`
  canonical identifier for the check definition
- `definition_language`
  one of `python` or `dsl`
- `supported_input_surfaces`
  [input surfaces](../concepts/runtime-model.md#input-surfaces) that can execute the check
- `required_context_paths`
  stable dotted paths inside [NormalizedContext](../concepts/runtime-model.md#normalized-context) that the check depends on
- `parity_baseline`
  whether the check participates in [strict comparison](../concepts/reference-and-parity.md#strict-comparison)
- `jurisdictions`
  markets where the check is eligible
- `legacy_identity`
  explicit mapping to the correct legacy emitted code template when the default mapping is not enough

Selection, validation, parity, and reporting all depend on this metadata.

## Selection inputs

The catalog selects checks by:

- input surface
- parity baseline
- jurisdictions
- optional explicit check ids

The public library APIs and application [check profiles](../concepts/check-model.md#check-profiles) use the same selection model.

## Parity baseline

`parity_baseline` answers one question: should this check participate in parity comparison?

- `legacy`
  the check is compared against legacy behavior
- `none`
  the check runs without legacy comparison

## Legacy identity

Checks compared against legacy behavior may need an explicit mapping to the legacy emitted code template. That mapping is stored as `legacy_identity`.

This lets the application compare one migrated check against the correct legacy tags even when the migrated implementation no longer mirrors the Perl source structure.

## Dependency invariant

For Python checks, declared `requires=(...)` metadata is validated against inferred context usage from the check function and helper annotations.

Helpers that receive `context` or any whole or intermediate context object must declare their dependency paths explicitly with `@depends_on_context_paths(...)`.

[Back to documentation](../index.md)
