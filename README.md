# Open Food Facts - Data Quality

This repository is a framework prototype for migrating Open Food Facts data quality checks from Perl to Python with parity validation against the legacy backend.

> [!NOTE]
> This prototype is still taking shape through active exploration.
> The current architecture and design decisions are not final and may change substantially as it is consolidated.
> Check back later to follow the project as it develops.

## Use the repository

You can use the repository in three ways:

- Use the shared runtime in `src/off_data_quality/` to run checks
  directly from Python. See
  [About the runtime model](docs/explanation/runtime-model.md).
- Use the migration tooling in `migration/` for complete runs. It loads a full
  product source snapshot and resolves
  [reference](docs/explanation/reference-data-and-parity.md) when the run
  needs it. It also handles
  [strict comparison](docs/explanation/reference-data-and-parity.md#strict-comparison),
  stored review data, and [report artifacts](docs/reference/report-artifacts.md).
- Use the Google Sheets demo in `apps/google_sheets/` when you want a browser
  flow that writes findings back to Google Sheets. See
  [Run the Google Sheets demo](apps/google_sheets/README.md).

## Run the migration demo

Use the migration demo image to inspect the full
[migration flow](docs/explanation/migration-runs.md) without cloning the
repository.

1. Run the published image:

   ```bash
   docker run --rm -p 8000:8000 ghcr.io/bobcorn/migration-demo
   ```

2. Open the report at `http://localhost:8000`.

The container loads the bundled sample snapshot, runs the shipped checks,
writes the report artifacts, and serves the generated site on port `8000`.

## Run the migration tooling locally

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

3. Build and start the migration tooling:

   ```bash
   docker compose up --build
   ```

4. Open the report at `http://localhost:8000`, unless you changed
   `MIGRATION_PORT` in `.env`.

For local run details, see
[Run the project locally](docs/how-to/run-the-project-locally.md).

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

Use Docker for migration runs that need reference results and for local
preview.

## Use the Python library

The public Python API exposes one concrete namespace:

- `off_data_quality.checks`

```python
from off_data_quality import checks

findings = checks.run(
    rows,
    check_ids=["en:serving-quantity-over-product-quantity"],
)
```

See [Use the Python library](docs/how-to/use-the-python-library.md) for
installation and usage details.

## Documentation

Read the full [documentation](docs/index.md).
