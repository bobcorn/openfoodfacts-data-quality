# Documentation

Use these pages based on what you need:

- Choose a how-to guide when you want to finish a task.
- Choose an explanation page when you want context, tradeoffs, or architecture.
- Choose a reference page when you need an exact contract, field list, or
  artifact description.

## How-to guides

- [Run the project locally](how-to/run-the-project-locally.md): Start the
  Docker workflow, choose the active dataset and check profiles, and set up
  `.venv` for local tooling.
- [Run the Google Sheets app](../apps/google_sheets/README.md): Run the local
  browser app that writes findings to Google Sheets.
- [Author checks](how-to/author-checks.md): Add a migrated check, choose `dsl`
  or `python`, and validate the change.
- [Use the Python library](how-to/use-the-python-library.md): Run checks
  directly from Python without the migration tooling.
- [Review a run report](how-to/review-a-run-report.md): Read the HTML report,
  interpret governed mismatches, and move to structured artifacts when you need
  more detail.
- [Troubleshoot local runs](how-to/troubleshoot-local-runs.md): Fix common
  Docker, snapshot, cache, registry, and backend issues.
- [Validate changes](how-to/validate-changes.md): Pick the right validation
  command for the risk of your change.

## Explanation

- [About the project scope](explanation/project-scope.md): Scope, stable
  boundaries, limits, and the role of the legacy backend.
- [About the runtime model](explanation/runtime-model.md): Shared runtime
  boundaries, context providers, and `CheckContext`.
- [About migrated checks](explanation/migrated-checks.md): Packaged checks,
  definition languages, metadata, and migration check profiles.
- [About reference and parity](explanation/reference-data-and-parity.md):
  Reference loading, cache reuse, strict comparison, and mismatch governance.
- [About the system architecture](explanation/system-architecture.md):
  Repository ownership boundaries across `src/`, `migration/`, `apps/`, and
  `ui/`.
- [About migration runs](explanation/migration-runs.md): The full run flow
  from source snapshot input and dataset selection to parity store data and
  report artifacts.

## Reference

- [Data contracts](reference/data-contracts.md): Input, runtime, reference, run,
  and review contracts loaded from stored runs.
- [Check metadata and selection](reference/check-metadata-and-selection.md):
  Executable metadata fields, catalog filters, and migration profile
  selection.
- [Run configuration and artifacts](reference/run-configuration-and-artifacts.md):
  Environment variables, check and dataset profiles, parity store settings,
  generated files, and cache details.
- [Report artifacts](reference/report-artifacts.md): HTML report structure,
  JSON artifact contents, and governance summaries loaded from the run store.
- [Legacy backend image](reference/legacy-backend-image.md): Source, published
  tags, scope, and cache fingerprint behavior.
- [CI and releases](reference/ci-and-releases.md): GitHub Actions workflows,
  triggers, and release outputs.
- [Glossary](reference/glossary.md): Canonical repository terms and naming
  rules.

## Project notes

- [Lessons learned from the prototype](project/lessons-from-the-prototype.md):
  Lessons learned while building the prototype.
- [Roadmap and open questions](project/roadmap-and-open-questions.md): Ongoing
  direction, open design questions, and risks to watch.
