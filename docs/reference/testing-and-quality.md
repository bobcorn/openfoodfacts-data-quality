# Testing and Quality

[Documentation](../index.md) / [Reference](index.md) / Testing and Quality

The default quality gate is small by design.

## Default Gate

```bash
make check
```

This runs the default repository gate. It covers formatting and linting, then runs the test suite with coverage.

## Broader Sweep

```bash
make quality
```

This adds stricter or broader advisory checks such as:

- `mypy`
- `vulture`
- `pyright`
- `jscpd`

## What The Tests Cover

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

- `make check` is the normal local and CI gate
- the broader tools are there to catch deeper issues, but they are not the day-to-day bottleneck

[Back to Reference](index.md) | [Back to Documentation](../index.md)
