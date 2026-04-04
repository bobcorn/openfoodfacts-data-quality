from __future__ import annotations

import hashlib
import json
import os
from typing import TYPE_CHECKING

import duckdb

from app.source.datasets import SourceSelection
from openfoodfacts_data_quality.contracts.raw import RawProductRow
from openfoodfacts_data_quality.source_rows import (
    PUBLIC_CSV_EXPORT_CONTRACT,
    PUBLIC_SOURCE_SNAPSHOT_CONTRACT,
    SUPPORTED_SOURCE_CONTRACTS,
    SupportedSourceContract,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

SOURCE_SNAPSHOT_ID_ENV_VAR = "SOURCE_SNAPSHOT_ID"


def source_snapshot_manifest_path_for(path: Path) -> Path:
    """Return the sidecar manifest path for one DuckDB source snapshot."""
    return path.with_suffix(f"{path.suffix}.snapshot.json")


def write_source_snapshot_manifest(
    path: Path,
    *,
    source_snapshot_id: str,
) -> Path:
    """Persist one explicit sidecar manifest for a DuckDB source snapshot."""
    normalized_snapshot_id = source_snapshot_id.strip()
    if not normalized_snapshot_id:
        raise ValueError("source_snapshot_id must not be blank.")
    manifest_path = source_snapshot_manifest_path_for(path)
    manifest_path.write_text(
        json.dumps({"source_snapshot_id": normalized_snapshot_id}, indent=2),
        encoding="utf-8",
    )
    return manifest_path


def source_snapshot_id_for(path: Path) -> str:
    """Return the configured or derived source snapshot identifier."""
    configured_snapshot_id = os.environ.get(SOURCE_SNAPSHOT_ID_ENV_VAR)
    if configured_snapshot_id is not None:
        normalized_snapshot_id = configured_snapshot_id.strip()
        if not normalized_snapshot_id:
            raise ValueError(
                f"{SOURCE_SNAPSHOT_ID_ENV_VAR} must not be blank when set."
            )
        return normalized_snapshot_id

    manifest_path = source_snapshot_manifest_path_for(path)
    if manifest_path.exists():
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        snapshot_id = str(payload.get("source_snapshot_id") or "").strip()
        if not snapshot_id:
            raise ValueError(
                f"Source snapshot manifest {manifest_path} must define "
                "'source_snapshot_id'."
            )
        return snapshot_id

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    source_snapshot_id = digest.hexdigest()[:12]
    write_source_snapshot_manifest(path, source_snapshot_id=source_snapshot_id)
    return source_snapshot_id


def count_source_rows(
    db_path: Path,
    *,
    selection: SourceSelection | None = None,
) -> int:
    """Return the number of products stored in the source DuckDB."""
    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        _require_supported_source_contract(connection)
        query, parameters = _source_query(
            selection=selection,
            select_list="count(*)",
            ordered=False,
        )
        result = connection.execute(query, parameters).fetchone()
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
    selection: SourceSelection | None = None,
) -> Iterator[list[RawProductRow]]:
    """Yield the products table as batches of validated raw contract rows."""
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")

    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        source_contract = _require_supported_source_contract(connection)
        select_list = ", ".join(
            _quote_identifier(column) for column in source_contract.required_columns
        )
        query, parameters = _source_query(
            selection=selection,
            select_list=select_list,
            ordered=True,
        )
        cursor = connection.execute(query, parameters)
        columns = [column[0] for column in cursor.description]
        while True:
            batch_rows = cursor.fetchmany(batch_size)
            if not batch_rows:
                break
            yield [
                source_contract.normalize_row(dict(zip(columns, row, strict=False)))
                for row in batch_rows
            ]
    finally:
        connection.close()


def _quote_identifier(value: str) -> str:
    """Quote a DuckDB identifier that may contain dashes."""
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def _source_query(
    *,
    selection: SourceSelection | None,
    select_list: str,
    ordered: bool,
) -> tuple[str, list[object]]:
    """Return the explicit source query and bound parameters for one selection."""
    if selection is None or selection.kind == "all_products":
        query = f"select {select_list} from products"
        if ordered:
            query += " order by code"
        return query, []

    if selection.kind == "stable_sample":
        if not ordered:
            return (
                """
                select least(count(*), ?)
                from products
                where code is not null
                  and trim(code) != ''
                """,
                [selection.sample_size],
            )
        query = f"""
            select {select_list}
            from products
            where code is not null
              and trim(code) != ''
            order by hash(code || ?), code
            limit ?
        """
        return query, [f"::{selection.seed}", selection.sample_size]

    placeholders = ", ".join("?" for _ in selection.codes)
    query = f"""
        select {select_list}
        from products
        where code in ({placeholders})
    """
    if ordered:
        query += " order by code"
    return query, list(selection.codes)


def _require_supported_source_contract(
    connection: duckdb.DuckDBPyConnection,
) -> SupportedSourceContract:
    """Return the matching source contract or raise a clear error."""
    available_columns = _available_source_columns(connection)
    for source_contract in SUPPORTED_SOURCE_CONTRACTS:
        if not source_contract.missing_columns(available_columns):
            return source_contract

    public_missing_csv = ", ".join(
        PUBLIC_SOURCE_SNAPSHOT_CONTRACT.missing_columns(available_columns)
    )
    csv_missing_csv = ", ".join(
        PUBLIC_CSV_EXPORT_CONTRACT.missing_columns(available_columns)
    )
    raise ValueError(
        "Source DuckDB does not satisfy any supported application source contract. "
        f"Missing public snapshot columns: {public_missing_csv}. "
        f"Missing public CSV export columns: {csv_missing_csv}. "
        "Use a public Open Food Facts source snapshot or a compatible public CSV export subset."
    )


def _available_source_columns(
    connection: duckdb.DuckDBPyConnection,
) -> set[str]:
    """Return the products table columns exposed by one source DuckDB."""
    return {
        row[0]
        for row in connection.execute("describe select * from products").fetchall()
    }
