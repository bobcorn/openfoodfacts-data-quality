[Back to documentation index](../index.md)

# Use the Python library

Use the Python library when you already have loaded rows and want findings
without migration source loading, reference caching, or report rendering.

## Install the package

The library is not published on PyPI.

To install it without a source checkout:

1. Open the
   [GitHub Releases page](https://github.com/bobcorn/openfoodfacts-data-quality/releases).
2. Download the wheel for the version you want. The file name looks like
   `openfoodfacts_data_quality-<VERSION>-py3-none-any.whl`.
3. Install the wheel with `pip`:

   ```bash
   python -m pip install /path/to/openfoodfacts_data_quality-<VERSION>-py3-none-any.whl
   ```

The wheel file uses the distribution name `openfoodfacts_data_quality`. After
installation, import the library from `off_data_quality`.

If you already have a source checkout, you can install the package from the
repository root instead:

```bash
python -m pip install .
```

## Import the API

The main public entry point is:

- `off_data_quality.checks`

It exposes:

- `checks.list(...)`
- `checks.prepare(...)`
- `checks.run(...)`

Import the public library from `off_data_quality`. In a source checkout, the
shared implementation and contracts live under `src/off_data_quality/`.

`off_data_quality.snapshots` remains reserved for a future direct enrichment
API, but it does not expose runnable entry points yet.

## Run on loaded rows

`checks.run(...)` accepts rows that use the canonical column names defined by
the library contract.

The library does not parse files for you. Load rows with the tools you prefer,
such as `csv`, DuckDB, pandas, or PyArrow, then pass those rows to
`checks.run(...)`.

`checks.run(...)` accepts row iterables and common table objects that are
already loaded in memory, such as pandas, PyArrow, and DuckDB objects. It
raises an error for file paths.

```python
from off_data_quality import checks

findings = checks.run(
    rows,
    check_ids=["en:serving-quantity-over-product-quantity"],
)
```

`checks.run(...)` prepares the rows before execution starts. If the rows do not
match a supported contract, it raises an error immediately.

Supported loaded inputs include:

- canonical rows, including sparse subsets and extra columns
- complete official OFF CSV export rows
- complete official OFF JSONL full documents
- complete official OFF Parquet rows
- DuckDB relations materialized from supported OFF rows

If you plan to run multiple check selections over the same loaded rows, prepare
them once first:

```python
from off_data_quality import checks

prepared_rows = checks.prepare(rows)
findings = checks.run(
    prepared_rows,
    check_ids=["en:serving-quantity-over-product-quantity"],
)
```

`checks.prepare(...)` can save repeated normalization work when you reuse the
same loaded rows across multiple runs. For a single run, `checks.run(...)`
already performs that preparation step internally.

For runnable walkthroughs that use the bundled sample data, see
[`examples/README.md`](../../examples/README.md).
That directory includes both plain Python examples in `examples/scripts/` and
paired Jupyter notebooks in `examples/notebooks/`.

## Remap column names explicitly

If your input uses different column names, pass an explicit `columns=...`
mapping.

- Key: canonical column name expected by the library
- Value: source column name present in your input rows

```python
from off_data_quality import checks

findings = checks.run(
    rows,
    columns={
        "code": "barcode",
        "product_name": "name",
        "quantity": "qty",
    },
)
```

The library does not infer aliases or read fallback shapes. If a mapped source
column is missing, the run fails fast. Extra columns that are not part of the
canonical contract are ignored.

Partial subsets of OFF structured export or document shapes are not treated as
supported OFF inputs. When you pass OFF-specific structured fields, the row
must match one supported OFF contract completely.

## Narrow the active check set

- Pass `check_ids` to run a specific subset.
- Pass [`jurisdictions`](../reference/check-metadata-and-selection.md#metadata-fields)
  to limit eligible checks.

If you request checks that need enriched snapshot input, the library fails
explicitly.

## Know what the library does not do

The public library does not include:

- source snapshot file loading from JSONL or DuckDB paths
- reference result loading or caching
- report generation
- artifact serialization
- [strict comparison](../explanation/reference-data-and-parity.md#strict-comparison)
  orchestration in migration tooling

Those responsibilities stay in the migration tooling.

## Choose the library or migration tooling

Use the library for programmatic findings inside Python callers.

Use migration tooling when you need
[source loading](../explanation/migration-runs.md#run-overview),
[reference](../explanation/reference-data-and-parity.md),
[strict comparison](../explanation/reference-data-and-parity.md#strict-comparison),
or the [HTML report](../reference/report-artifacts.md#html-report).

## Related information

- [About the runtime model](../explanation/runtime-model.md)
- [Data contracts](../reference/data-contracts.md)
- [CI and releases](../reference/ci-and-releases.md)
- [Run the project locally](run-the-project-locally.md)

[Back to documentation index](../index.md)
