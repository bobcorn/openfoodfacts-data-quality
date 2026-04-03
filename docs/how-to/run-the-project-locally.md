[Back to documentation index](../index.md)

# Run the project locally

Use this guide to start the Docker
[application flow](../explanation/application-runs.md) and set up local Python
tooling.

## Before you begin

- Docker
- Python 3.14 if you want a local `.venv`
- a local `.env` file

## Start the application

1. Create `.env` from the tracked sample file:

   ```bash
   cp .env.example .env
   ```

2. Build and start the local application flow:

   ```bash
   docker compose up --build
   ```

3. Open the local report at `http://localhost:8000`, unless you changed
   `PORT` in `.env`.

## Verify the result

On the first run, Docker builds the image, mounts the configured DuckDB
snapshot, writes
[artifacts](../reference/report-artifacts.md) under `artifacts/latest/`, and
serves the generated [report site](../reference/report-artifacts.md#html-report).

## Adjust run inputs

- `.env` holds local
  [run configuration](../reference/run-configuration-and-artifacts.md).
- `config/check-profiles.toml` defines named
  [check profiles](../explanation/migrated-checks.md#check-profiles).
- `DATABASE_PATH` selects the DuckDB
  [source snapshot](../reference/glossary.md#source-snapshot). Local runtime
  runs require it explicitly.
- `PORT` changes the host-side published port for the local preview.
- `BATCH_WORKERS` controls concurrent source-batch execution.
- `CHECK_PROFILE` chooses the active profile.
- `LEGACY_BACKEND_WORKERS` controls persistent backend workers for cache
  misses.

The starter `.env.example` points to the tracked sample DuckDB, so the first
run succeeds with repository data.

## Know when Docker is required

Compared runs and enriched application runs still depend on the
[reference path](../explanation/reference-data-and-parity.md#why-the-reference-path-exists)
and the supported [legacy backend environment](../reference/legacy-backend-image.md).

**Note:** Docker Compose does not mount the source tree into the container.
Rebuild with `docker compose up --build` after code changes.

## Set up `.venv`

Use a local `.venv` for tests, linting, typing, and repository utilities.

1. Create the virtual environment:

   ```bash
   python3.14 -m venv .venv
   ```

2. Install the repository with app and dev dependencies:

   ```bash
   .venv/bin/python -m pip install -e ".[app,dev]"
   ```

3. Run repository commands from that environment:

   ```bash
   .venv/bin/python -m pytest -q tests/test_some_area.py
   make check
   make quality
   ```

## Use quick local loops

- Narrow the active check set in `config/check-profiles.toml`.
- Keep the bundled sample snapshot for short compared runs.
- Switch to a larger local snapshot when you need wider coverage.
- Use `.venv` for focused Python loops and Docker for compared validation or
  report preview.

## Know what gets cached

- [Reference results](../explanation/reference-data-and-parity.md#why-the-reference-path-exists)
  are cached across runs to avoid repeated backend work.
- In the shipped Docker flow, that cache lives in the named
  `reference_result_cache` volume.
- Warm cache coverage can remove live backend execution for the covered
  products.
- [Source snapshots](../reference/glossary.md#source-snapshot) can carry a
  `.snapshot.json` sidecar. The runtime writes it automatically when it has to
  hash the DuckDB file.

## Related information

- [About application runs](../explanation/application-runs.md)
- [Run configuration and artifacts](../reference/run-configuration-and-artifacts.md)
- [Troubleshoot local runs](troubleshoot-local-runs.md)

[Back to documentation index](../index.md)
