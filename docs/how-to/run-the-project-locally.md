[Back to documentation index](../index.md)

# Run the project locally

Use this guide to start the Docker
[migration flow](../explanation/migration-runs.md) and set up local Python
tooling.

## Before you begin

- Docker
- Python 3.14 if you want a local `.venv`
- a local `.env` file

## Start the migration tooling

1. Create `.env` from the tracked sample file:

   ```bash
   cp .env.example .env
   ```

2. Review `.env` if you do not want the defaults. `CHECK_PROFILE` chooses which
   checks run. `SOURCE_DATASET_PROFILE` chooses which rows from the source
   snapshot enter the run.

3. Build and start the local migration flow:

   ```bash
   docker compose up --build
   ```

4. Open the local report at `http://localhost:8000`, unless you changed
   `MIGRATION_PORT` in `.env`. The local Compose flow binds to `127.0.0.1`
   by default.

## Verify the result

On the first run, Docker builds the image, mounts the configured source
snapshot, writes
[artifacts](../reference/report-artifacts.md) under `artifacts/latest/`, and
serves the generated [report site](../reference/report-artifacts.md#html-report).

The local command also records the run in the default
[parity store](../reference/run-configuration-and-artifacts.md#parity-store)
under `data/parity_store/parity.duckdb`, unless you changed
`PARITY_STORE_PATH`.

The store records review history across runs. It also supplies governance data
such as expected and unexpected mismatch counts when the report renders from a
recorded run.

## Adjust run inputs

- `.env` holds local
  [run configuration](../reference/run-configuration-and-artifacts.md).
- `config/check-profiles.toml` defines named
  [check profiles](../explanation/migrated-checks.md#check-profiles).
- `config/dataset-profiles.toml` defines named
  [dataset profiles](../reference/run-configuration-and-artifacts.md#dataset-profiles).
- `SOURCE_SNAPSHOT_PATH` selects the
  [source snapshot](../reference/glossary.md#source-snapshot). Local
  migration runs require it explicitly.
- `CHECK_PROFILE` chooses the active check profile.
- `SOURCE_DATASET_PROFILE` chooses the active source dataset profile.
- `MIGRATION_INVENTORY_PATH` and `MIGRATION_ESTIMATION_SHEET_PATH` point to
  optional migration planning metadata. The migration tooling uses it to filter runs
  by planning data and to show migration coverage in the report.
- `BATCH_WORKERS` controls batch concurrency.
- `LEGACY_BACKEND_WORKERS` controls persistent backend workers for cache
  misses.
- `MIGRATION_BIND_HOST` changes the host interface used by the local
  preview. The default is `127.0.0.1`.
- `MIGRATION_PORT` changes the preview port published on the host and passed to
  the preview server inside the local Compose flow.

The sample `.env.example` points to the tracked sample JSONL snapshot, so the
first run succeeds with repository data.

## Know when Docker is required

Compared runs and enriched snapshot migration runs still depend on the
[reference path](../explanation/reference-data-and-parity.md#why-the-reference-path-exists)
and the supported [legacy backend environment](../reference/legacy-backend-image.md).

Docker Compose does not mount the source tree into the container. Rebuild with
`docker compose up --build` after code changes.

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

- Start with `CHECK_PROFILE=focused` and `SOURCE_DATASET_PROFILE=smoke` when
  you want the shortest parity loop.
- Switch `SOURCE_DATASET_PROFILE` to `validation` when you want a broader
  deterministic review sample.
- Use `SOURCE_DATASET_PROFILE=benchmark_10k` or
  `.venv/bin/python scripts/benchmark_run.py --clear-reference-cache --repeat 2`
  when you want repeatable benchmark loops with machine-readable timing output.
- The benchmark JSON separates `run_preparation` timings from per-batch
  `stage_timings`, so you can distinguish source snapshot id and row-count
  costs from batch execution costs.
- Edit `config/check-profiles.toml` when you need a different set of checks.
- Keep the tracked sample snapshot for short compared runs.
- Switch to a larger local snapshot when you need wider coverage.
- Use `.venv` for focused Python loops and Docker for compared validation or
  report preview.

## Know what persists

- [Reference results](../explanation/reference-data-and-parity.md#why-the-reference-path-exists)
  are cached across runs to avoid repeated backend work.
- In the default Docker flow, that cache lives in the named
  `reference_result_cache` volume.
- A warm cache can avoid backend execution for covered products.
- `artifacts/latest/` is reset on every migration run.
- The parity store persists across runs and stores review history until you
  delete or replace its DuckDB file.
- [Source snapshots](../reference/glossary.md#source-snapshot) can include a
  `.snapshot.json` sidecar. The tracked example snapshots do not ship one. The
  runtime writes it automatically when it has to hash the source snapshot file.

## Related information

- [About migration runs](../explanation/migration-runs.md)
- [Run configuration and artifacts](../reference/run-configuration-and-artifacts.md)
- [Troubleshoot local runs](troubleshoot-local-runs.md)

[Back to documentation index](../index.md)
