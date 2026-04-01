# Testing and Quality

[Documentation](../index.md) / [Reference](index.md) / Testing and Quality

Repository validation model and main development commands.

Run repository Python commands from the repository local `.venv` when available.

## CI Gate

```bash
make check
```

CI uses this command.

It covers:

- formatting checks
- linting
- the test suite with coverage

## Local Sweep

```bash
make quality
```

This adds broader checks such as:

- `mypy`
- `vulture`
- `pyright`
- `jscpd`

For substantive local work, prefer this command.

## Test Coverage

The test suite covers the Python owned parts of the system, including:

- context construction
- check catalog and execution behavior
- DSL parsing and evaluation
- profile selection
- reference loading and caching behavior
- strict comparison and run result accumulation
- report rendering and serialization
- legacy inventory workflow support

## Quality Model

- `make check` is the repository CI gate and the minimum local validation pass
- `make quality` is the broader local sweep for deeper confidence
- changes that affect reference loading, strict comparison, or report generation may still need an end to end Docker run in addition to the Python toolchain

## Next Reads

- [Local Development](../guides/local-development.md)
- [Authoring Checks](../guides/authoring-checks.md)
- [CI and Releases](../operations/ci-and-releases.md)
