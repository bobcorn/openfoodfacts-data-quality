[Back to documentation index](../index.md)

# Review a run report

Use this guide when you need to understand the outcome of one
[application run](../explanation/application-runs.md).

## Before you begin

- a completed run under `artifacts/latest/site/`
- the HTML report or local preview site

## Open the report

1. Open `artifacts/latest/site/report.html`, or use the local preview started
   by [running the project locally](run-the-project-locally.md).
2. Read the page metadata first so you know which run and source snapshot you
   are reviewing.
3. Start at the
   [summary counters](../reference/report-artifacts.md#summary-counters)
   before you inspect one check card.

## Read the summary

1. Check `Mismatching` to see whether compared checks still diverge.
2. Check `Runtime Only` to see how much of the run had no
   [legacy baseline](../explanation/reference-data-and-parity.md#parity-baselines).
3. Check `Affected Products` to estimate how wide the retained mismatch sample
   is.
4. If the page includes `Policy Rules`, note that governance for expected
   differences
   was active for this run.

## Investigate one check

1. Open a mismatching check card.
2. Compare missing findings and extra findings.
3. If the card includes expected or unexpected differences, treat those as review
   annotations from the parity store. Expected means the mismatch is already
   known and tracked. It does not change pass or fail state.
4. Review retained mismatch examples for representative product codes.
5. Read implementation and legacy snippets when
   [snippet provenance](../reference/report-artifacts.md#snippetsjson) is
   available.

## Move to structured artifacts

- Use [`run.json`](../reference/report-artifacts.md#runjson) when you need the
  canonical run summary or programmatic diffs.
- Use [`snippets.json`](../reference/report-artifacts.md#snippetsjson) when you
  need structured snippet provenance.
- Use the ZIP archive when you want the two JSON artifacts together.

Remember that expected and unexpected mismatch governance counts live in the
report and in data from the parity store. They are not embedded in `run.json`.

## Know what the report does not show

The HTML report is a summary for review. It does not inline every
finding from the run, and mismatch examples are capped by the configured
retention budget.

## Related information

- [Report artifacts](../reference/report-artifacts.md)
- [About reference data and parity](../explanation/reference-data-and-parity.md)
- [Run the project locally](run-the-project-locally.md)

[Back to documentation index](../index.md)
