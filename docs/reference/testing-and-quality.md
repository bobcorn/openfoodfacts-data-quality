# Testing and Quality

[Documentation](../index.md) / [Reference](index.md) / Testing and Quality

Repository validation model and main development commands.

## CI Gate

```bash
make check
```

This is the normal repository validation gate used in CI.

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

For substantive local work, this is the better completion target even though the CI gate is smaller.

## Test Coverage

The test suite covers the Python-owned parts of the system, including:

- context construction
- check catalog and execution behavior
- DSL parsing and evaluation
- profile selection
- reference loading and caching behavior
- parity comparison and accumulation
- report rendering and serialization
- legacy inventory workflow support

## Quality Model

- `make check` is the repository CI gate and the minimum local validation pass
- `make quality` is the broader local sweep for deeper confidence
- parity-sensitive changes may still need a Docker-based end-to-end run in addition to the Python toolchain

## Next Reads

- [Local Development](../guides/local-development.md)
- [Authoring Checks](../guides/authoring-checks.md)
- [CI and Releases](../operations/ci-and-releases.md)
