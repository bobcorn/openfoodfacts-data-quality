# Reading The Report

[Documentation](../index.md) / [Getting Started](index.md) / Reading The Report

The application run layer writes a static report site and companion JSON artifacts under `artifacts/latest/site/`.

## Outputs

Main generated files:

- `index.html`
- `report.html`
- `run.json`
- `snippets.json`
- `openfoodfacts-data-quality-json.zip`

`index.html` and `report.html` currently carry the same rendered report. The JSON files are the structured artifacts. The ZIP file bundles those JSON outputs for convenience.

## HTML Report

The HTML report is the review summary for one quality run.

It answers questions like:

- which compared checks match exactly
- which compared checks still miss reference findings in migrated output
- which compared checks produce extra migrated findings
- which checks are runtime only
- which products appear in the retained mismatch examples

The report stays at summary level. It does not inline every finding from the run.

## Main Counters

- `Mismatching`
  Checks with at least one missing or extra finding under strict comparison.
- `Missing Findings`
  Findings present on the reference side but absent on the migrated side.
- `Extra Findings`
  Findings present on the migrated side but absent on the reference side.
- `Affected Products`
  Unique product codes involved in at least one retained mismatch example.
- `Runtime Only`
  Active checks that run without any legacy comparison baseline. This counter describes run composition, not pass or fail under strict comparison.

## Strict Comparison

Strict comparison is applied per compared check.

For each active check, the application compares reference and migrated findings as multisets over:

- product id
- observed code
- severity

As a result:

- duplicate occurrences matter
- dynamic emitted codes matter
- severity mismatches matter even when the check id is the same

Runtime only checks skip this comparison. They still appear in the report with their own outcome bucket.

## Check Cards

Each check card shows:

- the canonical check id
- the definition language, `python` or `dsl`
- the run outcome bucket
- matched, missing, and extra counts when comparison applies
- retained mismatch examples for each side
- code snippets for the current implementation and, when available, the matched legacy source

The mismatch examples are capped. They are examples, not a complete list.
If legacy source provenance is unavailable, the card still renders and explains that state in the snippet note area.

## `run.json`

`run.json` is the canonical JSON output of the application run model.

Use it when you want to:

- process run results after execution
- build other viewers or dashboards
- diff runs programmatically
- archive one run in a stable structured format

It stays closer to the application model than the HTML report does.

The root payload includes `kind` and `schema_version` metadata before the run body.

## `snippets.json`

`snippets.json` contains structured code excerpts keyed by check id.

It combines:

- implementation snippets from this repository
- legacy source snippets from the legacy source tree when they can be resolved

The snippet `origin` values are `implementation` and `legacy`.
Each check entry also includes `legacy_snippet_status` with one of:

- `available`
- `not_applicable`
- `unavailable`

Use it when you want provenance and review context without parsing HTML.

The root payload includes `kind`, `schema_version`, `issues`, and the `checks` mapping. Each check entry contains `legacy_snippet_status` and `snippets`.

## Report Scope

The report renderer supports both compared and runtime only checks.

Strict comparison counts and mismatch examples apply only to checks with a legacy baseline. Runtime only checks still contribute to run composition and to the report payload for each check.

## Next

- [Configuration and Artifacts](../operations/configuration-and-artifacts.md)
- [Application Run Flow](../architecture/application-run-flow.md)
- [Troubleshooting](troubleshooting.md)
