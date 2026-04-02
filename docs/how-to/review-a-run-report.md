# Review a Run Report

[Back to documentation](../index.md)

Use this guide when you need to understand the outcome of one [application run](../concepts/how-an-application-run-works.md).

## Before you start

- a completed run under `artifacts/latest/site/`
- the HTML report or local preview site

## Open the report

1. Open `artifacts/latest/site/report.html`, or use the local preview started by the [application flow](run-the-project-locally.md).
2. Start at the [summary counters](../reference/report-artifacts.md#summary-counters) before you inspect one check card.

## Read the summary

1. Check `Mismatching` to see whether compared checks still diverge.
2. Check `Runtime Only` to understand how much of the run had no [legacy baseline](../concepts/reference-and-parity.md#parity-baseline).
3. Check `Affected Products` to estimate how wide the retained mismatch sample is.

## Investigate one check

1. Open a mismatching check card.
2. Compare missing findings and extra findings.
3. Review retained mismatch examples for representative product codes.
4. Read implementation and legacy snippets when provenance is available.

## Move to structured artifacts

- Use [`run.json`](../reference/report-artifacts.md#runjson) when you need the full run summary or programmatic diffs.
- Use [`snippets.json`](../reference/report-artifacts.md#snippetsjson) when you need structured snippet provenance.
- Use the ZIP archive when you want both JSON artifacts together.

## Know what the report does not show

The HTML report keeps a summary aimed at review. It does not inline every finding from the run.

[Back to documentation](../index.md)
