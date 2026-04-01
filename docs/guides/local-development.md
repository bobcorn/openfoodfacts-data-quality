# Local Development

[Documentation](../index.md) / [Guides](index.md) / Local Development

Use Docker for parity work.

## Requirements

For the normal application workflow:

- Docker
- a local `.env` file

For Python only tooling outside Docker:

- Python
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
  Named check subsets for application runs.
- `DATABASE_PATH`
  The DuckDB snapshot used as the source input for the application.

The starter `.env.example` points to the tracked sample DuckDB so the first run succeeds with local repository data.

## Docker Workflow

Choose Docker for:

- parity execution
- the legacy backend runtime
- report generation
- a setup that mirrors the documented project workflow

Use Docker when working on application behavior.

The Docker image builds on a pinned image for multiple architectures. Normal Docker runs do not need a local `openfoodfacts-server` checkout. See [Legacy Backend Image](../operations/legacy-backend-image.md).

## `.venv` Workflow

Choose a local `.venv` for:

- focused tests
- linting and formatting
- typing checks
- utility scripts such as DSL validation

Setup:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[app,dev]"
```

Useful commands:

```bash
make check
make quality
pytest -q tests/test_some_area.py
python scripts/validate_dsl.py
```

## Development Loops

- edit `config/check-profiles.toml` to narrow the active check set
- use the bundled sample snapshot for short parity runs
- switch to a richer local snapshot when you need more representative behavior
- keep Docker for parity validation and use `.venv` for quicker Python only iterations

## Practical Notes

- The Docker Compose flow does not mount the source tree, so code changes require a rebuild.
- Reference side results are cached across runs to avoid repeating legacy backend work.

## Next Reads

- [Troubleshooting](../getting-started/troubleshooting.md)
- [Configuration and Artifacts](../operations/configuration-and-artifacts.md)
- [Legacy Backend Image](../operations/legacy-backend-image.md)
- [Reading The Report](../getting-started/reading-the-report.md)
