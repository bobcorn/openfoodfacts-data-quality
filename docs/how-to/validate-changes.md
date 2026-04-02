# Validate Changes

[Back to documentation](../index.md)

Choose the right validation command for repository work.

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
- [check catalog](../concepts/check-model.md#packaged-checks) and execution behavior
- DSL parsing and evaluation
- [check profile](../concepts/check-model.md#check-profiles) selection
- [reference path](../concepts/reference-and-parity.md#reference-path) loading and caching behavior
- [strict comparison](../concepts/reference-and-parity.md#strict-comparison) plus run result accumulation
- [report rendering](../reference/report-artifacts.md#html-report) and serialization

## Match validation to change risk

- `make check` is the CI gate and the minimum local pass.
- `make quality` is the default finish line for substantive changes.
- Changes that touch reference loading, strict comparison, report generation, or other full [application flow](../concepts/how-an-application-run-works.md) may still need a Docker run in addition to the Python toolchain.

[Back to documentation](../index.md)
