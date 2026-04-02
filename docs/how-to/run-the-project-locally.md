# Run the Project Locally

[Back to documentation](../index.md)

Start the [application flow](../concepts/how-an-application-run-works.md) in Docker and keep Python tooling in `.venv`.

## Before you begin

- Docker
- Python 3.14
- a local `.env` file

## Run the application

1. Create `.env` from the tracked sample file.

   ```bash
   cp .env.example .env
   ```

2. Build and start the local application flow.

   ```bash
   docker compose up --build
   ```

3. Open [http://localhost:8000](http://localhost:8000).

## Verify the result

The first run builds the image, mounts the configured DuckDB snapshot, writes artifacts under `artifacts/latest/`, and serves the generated report site.

## Adjust run inputs

- `.env` holds local [runtime configuration](../reference/run-configuration-and-artifacts.md).
- `config/check-profiles.toml` defines named [check profiles](../concepts/check-model.md#check-profiles).
- `DATABASE_PATH` selects the DuckDB [source snapshot](../reference/glossary.md#source-snapshot).
- `CHECK_PROFILE` chooses the active profile.
- `BATCH_WORKERS` controls application batch concurrency.
- `LEGACY_BACKEND_WORKERS` controls persistent backend workers for cache misses.

The starter `.env.example` points to the tracked sample DuckDB so the first run succeeds with repository data.

## Know when Docker is required

Compared runs and enriched application runs still depend on the [reference path](../concepts/reference-and-parity.md#reference-path) and the supported [legacy backend environment](../reference/legacy-backend-image.md).

> Note
> Docker Compose does not mount the source tree into the container. Rebuild with `docker compose up --build` after code changes.

## Set up `.venv`

Use a local `.venv` for tests, linting, typing, and repository utilities.

1. Create the virtual environment.

   ```bash
   python3.14 -m venv .venv
   ```

2. Install the repository with app and dev dependencies.

   ```bash
   .venv/bin/python -m pip install -e ".[app,dev]"
   ```

3. Run repository commands from the local environment.

   ```bash
   .venv/bin/python -m pytest -q tests/test_some_area.py
   make check
   make quality
   ```

## Use quick local loops

- narrow the active check set in `config/check-profiles.toml`
- keep the bundled sample snapshot for short compared runs
- switch to a larger local snapshot when you need wider coverage
- use `.venv` for focused Python loops and Docker for compared validation or report preview

## Know what gets cached

- [Reference results](../concepts/reference-and-parity.md#reference-path) are cached across runs to avoid repeated backend work.
- Warm cache coverage can remove live backend execution for the covered products.
- [Source snapshots](../reference/glossary.md#source-snapshot) can carry a `.snapshot.json` sidecar. The runtime writes it automatically when it has to hash the DuckDB file.

[Back to documentation](../index.md)
