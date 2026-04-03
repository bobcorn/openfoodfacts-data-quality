[Back to documentation index](../index.md)

# Report artifacts

This page describes the files produced under `artifacts/latest/site/` and what
each one contains.

## Generated files

- `index.html`
- `report.html`
- `run.json`
- `snippets.json`
- `openfoodfacts-data-quality-json.zip`

The JSON files are the structured artifacts. The ZIP file bundles those JSON
outputs for download.

`index.html` and `report.html` render the same content.

## HTML report

The HTML report is the review summary for one
[application run](../explanation/application-runs.md).

It shows which compared checks match, which still have missing or extra
findings, which checks are runtime only, and which products appear in retained
mismatch examples.

The report stays at summary level. It does not inline every finding from the
run.

## Summary counters

- `Mismatching`: Checks with at least one missing or extra finding under
  [strict comparison](../explanation/reference-data-and-parity.md#strict-comparison)
- `Missing Findings`: Findings present on the reference side and absent on the
  migrated side
- `Extra Findings`: Findings present on the migrated side and absent on the
  reference side
- `Affected Products`: Unique product codes that appear in retained mismatch
  examples
- `Runtime Only`: Active checks with no
  [legacy comparison baseline](../explanation/reference-data-and-parity.md#parity-baselines)

## Check cards

Each check card shows:

- the canonical check id
- the [definition language](../explanation/migrated-checks.md#definition-languages),
  `python` or `dsl`
- the run outcome bucket
- matched, missing, and extra counts when comparison applies
- retained mismatch examples for each side
- code snippets for the current implementation and, when available, the matched
  legacy source

If legacy source provenance is unavailable, the card still renders and explains
that state in the snippet area.

Retained mismatch examples are capped. Use `run.json` when you need the full
structured run summary.

## `run.json`

`run.json` is the canonical JSON output of the
[application run model](data-contracts.md#runresult).

Use it when you want to:

- process run results after execution
- build another viewer or dashboard
- diff runs programmatically
- archive one run in a stable structured format

The root payload includes `kind` and `schema_version` metadata before the run
body.

## `snippets.json`

`snippets.json` stores structured code excerpts keyed by check id.

It includes:

- implementation snippets from this repository
- legacy source snippets when they can be resolved

The snippet `origin` values are `implementation` and `legacy`.

Each check entry also includes `legacy_snippet_status` with one of these
values:

- `available`
- `not_applicable`
- `unavailable`

Use `snippets.json` when you want provenance and review context without parsing
HTML.

## Scope

The renderer supports compared checks and checks that run without comparison in
the same run.

Strict comparison counts and mismatch examples apply only to checks with a
[legacy baseline](../explanation/reference-data-and-parity.md#parity-baselines).
Checks that run without comparison still contribute to run composition and per
check output.

## See also

- [Review a run report](../how-to/review-a-run-report.md)
- [About application runs](../explanation/application-runs.md)
- [Run configuration and artifacts](run-configuration-and-artifacts.md)

[Back to documentation index](../index.md)
