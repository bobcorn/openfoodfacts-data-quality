from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

import duckdb

from openfoodfacts_data_quality.raw_products import RAW_INPUT_COLUMNS

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


def source_snapshot_id_for(path: Path) -> str:
    """Derive a short deterministic source snapshot id from the DuckDB file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()[:12]


def count_source_rows(db_path: Path) -> int:
    """Return the number of products stored in the source DuckDB."""
    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        _require_source_columns(connection)
        result = connection.execute("select count(*) from products").fetchone()
        if result is None:
            raise RuntimeError(
                "DuckDB did not return a row count for the products table."
            )
        return int(result[0])
    finally:
        connection.close()


def iter_source_batches(
    db_path: Path,
    *,
    batch_size: int,
) -> Iterator[list[dict[str, Any]]]:
    """Yield the products table as batches of plain dictionaries ordered by code."""
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")

    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        _require_source_columns(connection)
        select_list = ", ".join(
            _quote_identifier(column) for column in RAW_INPUT_COLUMNS
        )
        cursor = connection.execute(f"select {select_list} from products order by code")
        columns = [column[0] for column in cursor.description]
        while True:
            batch_rows = cursor.fetchmany(batch_size)
            if not batch_rows:
                break
            yield [dict(zip(columns, row, strict=False)) for row in batch_rows]
    finally:
        connection.close()


def _quote_identifier(value: str) -> str:
    """Quote a DuckDB identifier that may contain dashes."""
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def _require_source_columns(connection: duckdb.DuckDBPyConnection) -> None:
    """Raise a clear error when the local DuckDB snapshot does not match the source contract."""
    available_columns = {
        row[0]
        for row in connection.execute("describe select * from products").fetchall()
    }
    missing_columns = [
        column for column in RAW_INPUT_COLUMNS if column not in available_columns
    ]
    if not missing_columns:
        return

    missing_csv = ", ".join(missing_columns)
    raise ValueError(
        "Source DuckDB does not satisfy the explicit parity source contract. "
        f"Missing columns: {missing_csv}. "
        "Regenerate the sample DuckDB or align the input snapshot schema with "
        "openfoodfacts_data_quality.raw_products.RAW_INPUT_COLUMNS."
    )
