# Local Development

[Documentation](../index.md) / [Guides](index.md) / Local Development

Use Docker for application runs that need backend [reference data](../architecture/application-run-flow.md).

## Requirements

For application runs:

- Docker
- a local `.env` file

For Python tooling outside Docker:

- Python 3.14
- a local virtual environment

## First Local Run

```bash
cp .env.example .env
docker compose up --build
```

Then open [http://localhost:8000](http://localhost:8000).

The first local run:

- builds the runtime image
- mounts the configured DuckDB snapshot
- persists the reference result cache
- writes report artifacts under `artifacts/latest/`
- serves the generated report site

## Local Inputs

- `.env`
  Local runtime configuration.
- `config/check-profiles.toml`
  Named [check subsets](../operations/configuration-and-artifacts.md) for application runs.
- `DATABASE_PATH`
  The DuckDB [source snapshot](../glossary.md) used as the source input for the application.
- `BATCH_WORKERS`
  Concurrent batch workers for the [application run loop](../architecture/application-run-flow.md).
- `LEGACY_BACKEND_WORKERS`
  Persistent [legacy backend](../architecture/application-run-flow.md#legacy-backend) workers used only for cache misses that need backend materialization.

The starter `.env.example` points to the tracked sample DuckDB so the first run succeeds with local repository data.

## Docker

Keep Docker for runs that may need [reference results](../architecture/application-run-flow.md#reference-path), compared execution, enriched application runs, [report generation](../getting-started/reading-the-report.md), and preview.

The Docker image builds on a pinned image for multiple architectures. Normal Docker runs do not need a local `openfoodfacts-server` checkout. See [Legacy Backend Image](../operations/legacy-backend-image.md).

## `.venv` Workflow

Use a local `.venv` for:

- focused tests
- linting and formatting
- typing checks
- utility scripts such as DSL validation

Setup:

```bash
python3.14 -m venv .venv
.venv/bin/python -m pip install -e ".[app,dev]"
```

Useful commands:

```bash
.venv/bin/python -m pytest -q tests/test_some_area.py
.venv/bin/python scripts/validate_dsl.py
make check
make quality
```

## Short Loops

- edit `config/check-profiles.toml` to narrow the active check set
- use the bundled sample snapshot for short compared runs
- switch to a richer local snapshot when you need more representative behavior
- keep Docker for compared validation and use `.venv` for quicker Python only iterations

## Practical Notes

- The Docker Compose flow does not mount the source tree. Code changes require a rebuild.
- [Reference side results](../architecture/application-run-flow.md#reference-path) are cached across runs to avoid repeating legacy backend work.
- With a warm cache, compared and enriched runs can complete without starting a live backend worker for the covered products.
- [Source snapshot](../glossary.md) ids can come from a `.snapshot.json` sidecar beside the DuckDB file. The runtime writes that sidecar automatically when it has to hash the DuckDB file directly.

## Next

- [Troubleshooting](../getting-started/troubleshooting.md)
- [Configuration and Artifacts](../operations/configuration-and-artifacts.md)
- [Legacy Backend Image](../operations/legacy-backend-image.md)
- [Reading The Report](../getting-started/reading-the-report.md)
