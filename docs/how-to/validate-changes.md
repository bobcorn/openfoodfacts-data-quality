[Back to documentation index](../index.md)

# Validate changes

Use this guide to choose the right validation command for repository work.

Run repository Python commands from the local `.venv` when it exists.

## Run the CI gate

Run the minimum validation pass from the repository root:

```bash
make check
```

This command runs format checks, linting, and the test suite with coverage.

## Run the full local sweep

Run the broader local sweep before you call substantive work done:

```bash
make quality
```

This command adds `mypy`, `vulture`, `pyright`, and `jscpd`.

## Know what the test suite covers

- context construction
- [check catalog](../explanation/migrated-checks.md#packaged-checks) and
  execution behavior
- [DSL](../explanation/migrated-checks.md#definition-languages) parsing and
  evaluation
- [check profile](../explanation/migrated-checks.md#check-profiles) and dataset
  profile selection
- migration catalog loading and planning filters
- [reference path](../explanation/reference-data-and-parity.md#why-the-reference-path-exists)
  loading and caching behavior
- [strict comparison](../explanation/reference-data-and-parity.md#strict-comparison)
  plus run result accumulation
- parity store persistence and report rendering from stored runs
- [report rendering](../reference/report-artifacts.md#html-report) and
  serialization

## Match validation to change risk

- `make check` is the CI gate and the minimum local pass.
- `make quality` is the default finish line for substantive changes.
- Changes that touch reference loading, strict comparison, report generation,
  or other full
  [application flow](../explanation/application-runs.md#run-overview) may still
  need a Docker run in addition to the Python toolchain.

## Related information

- [Author checks](author-checks.md)
- [Run the project locally](run-the-project-locally.md)
- [About application runs](../explanation/application-runs.md)

[Back to documentation index](../index.md)
