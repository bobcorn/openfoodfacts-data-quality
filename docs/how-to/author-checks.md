[Back to documentation index](../index.md)

# Author checks

Use this guide to add a migrated check and keep the logic inside the
[shared runtime](../explanation/runtime-model.md#why-the-runtime-is-split).

## Before you begin

- Choose the [context provider](../explanation/runtime-model.md#context-providers)
  the rule actually needs.
- Decide whether the check should use a
  [`parity_baseline`](../explanation/reference-data-and-parity.md#parity-baselines)
  of `legacy` or `none`.
- Choose the
  [definition language](../explanation/migrated-checks.md#definition-languages)
  that makes the rule readable.

## Follow the workflow

```mermaid
flowchart TB
    A["Choose check target"]
    B["Choose DSL or Python"]
    C["Declare metadata"]
    D["Add tests"]
    E["Run focused validation"]
    F["Review report or artifacts"]
    G["Iterate"]
    H["Keep in shared runtime"]

    A --> B --> C --> D --> E --> F
    F -->|"Needs changes"| G
    G --> E
    F -->|"Ready"| H
```

1. Choose the rule and the provider it belongs to.
2. Choose `dsl` or `python`.
3. Declare [metadata](../reference/check-metadata-and-selection.md) that
   matches the rule contract.
4. Add or update tests.
5. Run a narrow validation loop.
6. Review report output or [JSON artifacts](../reference/report-artifacts.md)
   when parity applies.
7. Keep the final definition in the
   [shared runtime](../explanation/runtime-model.md#why-the-runtime-is-split),
   not in `migration/`.

## Choose the definition language

Use the [DSL](../explanation/migrated-checks.md#definition-languages) when the
rule is a readable boolean predicate over approved
[CheckContext](../reference/data-contracts.md#checkcontext) paths and
one static severity is enough.

Use Python when the rule needs:

- loops or aggregation
- logic that depends on helpers
- numeric reasoning across multiple steps
- dynamic emitted codes

## Put the definition in the right pack

- Python checks live under `src/off_data_quality/checks/packs/python/`.
- DSL checks live under `src/off_data_quality/checks/packs/dsl/`.

Checks are packaged repository content. Do not hide them in `migration/`.

## Set the metadata

- `required_context_paths` records the stable dotted paths the rule needs
  inside [`CheckContext`](../reference/data-contracts.md#checkcontext).
- `parity_baseline` decides whether the check enters
  [strict comparison](../explanation/reference-data-and-parity.md#strict-comparison).
- `jurisdictions` limits the markets where the rule is eligible.
- `legacy_identity` is derived from the check id for checks that participate in
  parity comparison.

For Python checks, declare the stable context paths with `requires=(...)`. The
catalog materializes that declaration as `required_context_paths`. The catalog
validates that each declared path exists; it does not infer dependencies from
Python source.

## Validate the change

1. Add or update tests.
2. If you changed DSL files, validate the DSL packs:

   ```bash
   .venv/bin/python scripts/validate_dsl.py
   ```

   This command validates the shared JSON Schema and the semantic rules for
   repository DSL packs.

3. Use a focused
   [check profile](../explanation/migrated-checks.md#check-profiles) when you
   work on a parity check.
4. Finish with the repository sweep:

   ```bash
   make quality
   ```

If the change touches the full migration flow, reference loading, strict
comparison, or report output, also run the Docker
[migration flow](../explanation/migration-runs.md#run-overview) before you
call the work done.

## Extend contracts on purpose

Checks depend on stable
[`CheckContext`](../reference/data-contracts.md#checkcontext) paths,
not on helper shapes local to `migration/`. Helpers may receive leaf values, sections,
or the whole context, but the check decorator still owns the dependency
contract through `requires`.

If a check needs additional stable data, extend the
[normalized contract](../reference/data-contracts.md#checkcontext) and
update tests plus docs in the same task.

## Use editor support for DSL packs

- `.vscode/settings.json` associates DSL YAML files with the schema.
- `.vscode/tasks.json` provides `Validate DSL checks` and `Watch DSL checks`.

## Related information

- [About migrated checks](../explanation/migrated-checks.md)
- [About the runtime model](../explanation/runtime-model.md)
- [Validate changes](validate-changes.md)

[Back to documentation index](../index.md)
