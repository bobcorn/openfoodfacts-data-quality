# Reading The Report

[Documentation](../index.md) / [Getting Started](index.md) / Reading The Report

The parity application writes a static report site and companion JSON artifacts under `artifacts/latest/site/`.

Use this page to read those outputs quickly.

## Run Outputs

Main generated files:

- `index.html`
- `report.html`
- `parity.json`
- `snippets.json`
- `openfoodfacts-data-quality-json.zip`

`index.html` and `report.html` currently carry the same rendered report. The JSON files are the machine-readable artifacts. The ZIP file bundles those JSON outputs for convenience.

## HTML Report

The HTML report is the reviewer-facing summary for one parity run.

It answers questions like:

- which checks match exactly
- which checks still miss reference findings in migrated output
- which checks produce extra migrated findings
- which products appear in the retained mismatch examples

The report is intentionally summary-first. It does not try to inline every finding from the run.

## Main Counters

- `Checks Mismatching`
  Checks with at least one missing or extra finding under strict parity.
- `Missing Findings`
  Findings present on the reference side but absent on the migrated side.
- `Extra Findings`
  Findings present on the migrated side but absent on the reference side.
- `Affected Products`
  Unique product codes involved in at least one retained mismatch example.

## Parity Comparison

Parity is strict and check-local.

For each active check, the application compares reference and migrated findings as multisets over:

- product id
- observed code
- severity

As a result:

- duplicate occurrences matter
- dynamic emitted codes matter
- severity mismatches matter even when the check id is the same

## Check Cards

Each check card shows:

- the canonical check id
- the definition language, `python` or `dsl`
- the parity outcome bucket
- matched, missing, and extra counts
- retained mismatch examples for each side
- code snippets for the migrated implementation and, when available, the matched legacy source

The mismatch examples are capped. They are illustrative, not exhaustive.

## `parity.json`

`parity.json` is the canonical machine-readable output of the parity domain model.

Use it when you want to:

- post-process parity results
- build other viewers or dashboards
- diff runs programmatically
- archive one run in a stable structured format

It stays closer to the application model than the HTML report does.

## `snippets.json`

`snippets.json` contains structured code excerpts keyed by check id.

It combines:

- migrated Python or DSL snippets from this repository
- legacy Perl snippets from the legacy source tree when they can be resolved

Use it when you want provenance and review context without parsing HTML.

## Current Limitation

The current report renderer expects only parity-compared checks.

That means the report is aimed at legacy-backed migration runs. Runtime-only checks with `parity_baseline="none"` are supported by the shared library and the check catalog, but they are not part of the current migration-report presentation flow.

## Next Reads

- [Configuration and Artifacts](../operations/configuration-and-artifacts.md)
- [Parity Pipeline](../architecture/parity-pipeline.md)
- [Troubleshooting](troubleshooting.md)
