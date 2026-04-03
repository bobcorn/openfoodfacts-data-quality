# Open Food Facts - Data Quality

This repository is a framework prototype for migrating Open Food Facts data quality checks from Perl to Python with parity validation against the legacy backend.

You can use the repository in two ways:

- Use the shared runtime in `src/openfoodfacts_data_quality/` to run checks
  directly from Python. See
  [About the runtime model](docs/explanation/runtime-model.md).
- Use the application layer in `app/` for complete runs. It loads DuckDB input and
  resolves [reference data](docs/explanation/reference-data-and-parity.md) when
  the run needs it. It also handles
  [strict comparison](docs/explanation/reference-data-and-parity.md#strict-comparison),
  stored review data, and [report artifacts](docs/reference/report-artifacts.md).

For a deeper architectural view, see
[About the system architecture](docs/explanation/system-architecture.md) and
[About application runs](docs/explanation/application-runs.md).

## Run the demo

Use the demo image to inspect the full
[application flow](docs/explanation/application-runs.md) without cloning the
repository.

1. Run the published image:

   ```bash
   docker run --rm -p 8000:8000 ghcr.io/bobcorn/openfoodfacts-data-quality:demo
   ```

2. Open the report at `http://localhost:8000`.

The container loads the bundled sample snapshot, runs the shipped checks,
writes the report artifacts, and serves the generated site on port `8000`.

## Run the application locally

Use this procedure when you work in the repository.

1. Clone the repository and enter the working tree:

   ```bash
   git clone https://github.com/bobcorn/openfoodfacts-data-quality.git
   cd openfoodfacts-data-quality
   ```

2. Create `.env` from the tracked sample file:

   ```bash
   cp .env.example .env
   ```

3. Build and start the application:

   ```bash
   docker compose up --build
   ```

4. Open the report at `http://localhost:8000`, unless you changed
   `PORT` in `.env`.

The sample `.env.example` points `SOURCE_SNAPSHOT_PATH` at the tracked DuckDB
snapshot. `CHECK_PROFILE` selects the active checks. `SOURCE_DATASET_PROFILE`
selects the source rows that enter the run. Outputs go under
`artifacts/latest/`. The application records review data in
`data/parity_store/parity.duckdb`. The Docker flow also reuses the
[reference result cache](docs/reference/run-configuration-and-artifacts.md#reference-result-cache)
across runs.

If `SOURCE_SNAPSHOT_PATH` is unset, local runtime startup fails instead of
falling back to a bundled path. Use the demo image for the bundled sample
without local configuration.

## Set up local Python tooling

Use a local `.venv` for tests, linting, typing, and repository utilities.

1. Create the virtual environment:

   ```bash
   python3.14 -m venv .venv
   ```

2. Install the repository with app and dev dependencies:

   ```bash
   .venv/bin/python -m pip install -e ".[app,dev]"
   ```

3. Run local commands from that environment:

   ```bash
   .venv/bin/pytest -q tests/test_some_area.py
   make quality
   ```

Use Docker for application runs that need reference results and for local
preview.

## Use the Python library

The public Python API exposes two input surfaces:

- `openfoodfacts_data_quality.raw`
- `openfoodfacts_data_quality.enriched`

Use `raw` when the rule depends only on public product rows:

```python
from openfoodfacts_data_quality import raw

findings = raw.run_checks(
    rows,
    check_ids=["en:serving-quantity-over-product-quantity"],
)
```

Use `enriched` when a check depends on stable enriched data. In application
runs, that data usually comes from the
[reference path](docs/explanation/reference-data-and-parity.md#why-the-reference-path-exists).
When you call the library directly, provide
[EnrichedSnapshotResult](docs/reference/data-contracts.md#enrichedsnapshotresult)
values explicitly.

## Documentation

Start with the [documentation index](docs/index.md).

- Use the [how-to guides](docs/index.md#how-to-guides) for tasks.
- Use the [explanation pages](docs/index.md#explanation) for architecture and
  design context.
- Use the [reference pages](docs/index.md#reference) for contracts, artifacts,
  configuration, and exact field definitions.
