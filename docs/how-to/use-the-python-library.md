[Back to documentation index](../index.md)

# Use the Python library

Use the Python library when you already have loaded rows and want findings
without application source loading, reference caching, or report rendering.

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

If you already have a source checkout, you can install the package from the
repository root instead:

```bash
python -m pip install .
```

## Import the API

The current public entry point is:

- `off_data_quality.checks`

It exposes:

- `checks.list(...)`
- `checks.run(...)`

Import the public library from `off_data_quality`. The shared implementation
and contracts live under `openfoodfacts_data_quality/` inside `src/`.

`off_data_quality.snapshots` exists as a placeholder namespace for a future
direct enrichment API, but it does not expose runnable entry points yet.

## Run on loaded rows

`checks.run(...)` accepts rows that use the canonical column names defined by
the library contract.

The library does not parse files for you. Load rows with the tools you prefer,
such as `csv`, DuckDB, pandas, or PyArrow, then pass those rows to
`checks.run(...)`.

`checks.run(...)` accepts row iterables and common table-like objects that are
already loaded in memory, such as pandas-style, PyArrow-style, and DuckDB-style
objects. It also normalizes the structured Open Food Facts product export
shape. It raises an error for file paths.

```python
from off_data_quality import checks

findings = checks.run(
    rows,
    check_ids=["en:serving-quantity-over-product-quantity"],
)
```

`checks.run(...)` prepares the rows before execution starts. If the rows do not
match a supported contract, it raises an error immediately.

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
  orchestration in the application

Those responsibilities stay in the application layer.

## Choose the library or the application

Use the library for programmatic findings inside Python callers.

Use the application when you need
[source loading](../explanation/application-runs.md#run-overview),
[reference](../explanation/reference-data-and-parity.md),
[strict comparison](../explanation/reference-data-and-parity.md#strict-comparison),
or the [HTML report](../reference/report-artifacts.md#html-report).

## Related information

- [About the runtime model](../explanation/runtime-model.md)
- [Data contracts](../reference/data-contracts.md)
- [CI and releases](../reference/ci-and-releases.md)
- [Run the project locally](run-the-project-locally.md)

[Back to documentation index](../index.md)
