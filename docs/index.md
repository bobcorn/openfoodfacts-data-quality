# Documentation

Use these pages based on what you need right now:

- Choose a how-to guide when you want to finish a task.
- Choose an explanation page when you want context, tradeoffs, or architecture.
- Choose a reference page when you need an exact contract, field list, or artifact description.

## How-to guides

- [Run the project locally](how-to/run-the-project-locally.md): Start the Docker workflow, verify the result, and set up `.venv` for local tooling.
- [Author checks](how-to/author-checks.md): Add a migrated check, choose `dsl` or `python`, and validate the change.
- [Use the Python library](how-to/use-the-python-library.md): Run checks directly from Python without the application layer.
- [Review a run report](how-to/review-a-run-report.md): Read the HTML report and move to the JSON artifacts when you need more detail.
- [Troubleshoot local runs](how-to/troubleshoot-local-runs.md): Fix common Docker, snapshot, cache, and backend issues.
- [Validate changes](how-to/validate-changes.md): Pick the right validation command for the risk of your change.

## Explanation

- [About the project scope](explanation/project-scope.md): Scope, stable boundaries, current limits, and the role of the legacy backend.
- [About the runtime model](explanation/runtime-model.md): Shared runtime boundaries, input surfaces, and `NormalizedContext`.
- [About migrated checks](explanation/migrated-checks.md): Packaged checks, definition languages, metadata, and check profiles.
- [About reference data and parity](explanation/reference-data-and-parity.md): Reference loading, cache reuse, strict comparison, and parity baselines.
- [About the system architecture](explanation/system-architecture.md): Repository ownership boundaries and the split between `src/` and `app/`.
- [About application runs](explanation/application-runs.md): The full run flow from DuckDB input to report artifacts.

## Reference

- [Data contracts](reference/data-contracts.md): Input, runtime, reference, and output contracts.
- [Check metadata and selection](reference/check-metadata-and-selection.md): Executable metadata fields and selection inputs.
- [Run configuration and artifacts](reference/run-configuration-and-artifacts.md): Environment variables, check profiles, generated files, and cache details.
- [Report artifacts](reference/report-artifacts.md): HTML report structure and JSON artifact contents.
- [Legacy backend image](reference/legacy-backend-image.md): Source, published tags, scope, and cache fingerprint behavior.
- [CI and releases](reference/ci-and-releases.md): GitHub Actions workflows, triggers, and release outputs.
- [Glossary](reference/glossary.md): Canonical repository terms and naming rules.

## Project notes

- [Roadmap and open questions](project/roadmap-and-open-questions.md): Current direction, open design questions, and risks to watch.
