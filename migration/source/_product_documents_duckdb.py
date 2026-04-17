from __future__ import annotations

from collections.abc import Collection, Iterator
from pathlib import Path

import duckdb

from migration.source._product_documents_common import (
    MISSING_OR_BLANK_CODE_REASON,
    SKIPPED_SOURCE_ROW_EXAMPLE_LIMIT,
    required_selection_sample_size,
    required_selection_seed,
)
from migration.source.datasets import SourceSelection
from migration.source.models import (
    SkippedSourceRow,
    SourceBatchRecord,
    SourceInputSummary,
)
from migration.source.product_documents import source_batch_record_from_document


class DuckDBProductDocumentAdapter:
    """Read full product documents from one DuckDB products table."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def summarize_input(
        self,
        *,
        selection: SourceSelection | None = None,
    ) -> SourceInputSummary:
        """Return source input diagnostics for the selected DuckDB rows."""
        connection = duckdb.connect(str(self.path), read_only=True)
        try:
            _require_code_column(
                _available_source_columns(connection), source=self.path
            )
            if selection is None or selection.kind == "all_products":
                return _summarize_duckdb_all_products(
                    connection,
                    source=self.path,
                )

            query, parameters = _duckdb_source_query(
                selection=selection,
                select_list="count(*)",
                ordered=False,
            )
            result = connection.execute(query, parameters).fetchone()
            if result is None:
                raise RuntimeError(
                    "DuckDB did not return a row count for the products table."
                )
            return SourceInputSummary(processed_product_count=int(result[0]))
        finally:
            connection.close()

    def iter_batches(
        self,
        *,
        batch_size: int,
        selection: SourceSelection | None = None,
    ) -> Iterator[list[SourceBatchRecord]]:
        """Yield DuckDB product documents as source batch records."""
        if batch_size <= 0:
            raise ValueError("batch_size must be a positive integer.")

        connection = duckdb.connect(str(self.path), read_only=True)
        try:
            columns = tuple(sorted(_available_source_columns(connection)))
            _require_code_column(columns, source=self.path)
            select_list = ", ".join(_quote_identifier(column) for column in columns)
            query, parameters = _duckdb_source_query(
                selection=selection,
                select_list=select_list,
                ordered=True,
            )
            cursor = connection.execute(query, parameters)
            cursor_columns = [column[0] for column in cursor.description]
            while True:
                batch_rows = cursor.fetchmany(batch_size)
                if not batch_rows:
                    break
                yield [
                    source_batch_record_from_document(
                        dict(zip(cursor_columns, row, strict=True))
                    )
                    for row in batch_rows
                ]
        finally:
            connection.close()


def _duckdb_source_query(
    *,
    selection: SourceSelection | None,
    select_list: str,
    ordered: bool,
) -> tuple[str, list[object]]:
    if selection is None or selection.kind == "all_products":
        query = f"""
            select {select_list}
            from products
            where code is not null
              and trim(code) != ''
        """
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
                [required_selection_sample_size(selection)],
            )
        query = f"""
            select {select_list}
            from products
            where code is not null
              and trim(code) != ''
            order by hash(code || ?), code
            limit ?
        """
        return (
            query,
            [
                f"::{required_selection_seed(selection)}",
                required_selection_sample_size(selection),
            ],
        )

    placeholders = ", ".join("?" for _ in selection.codes)
    query = f"""
        select {select_list}
        from products
        where code in ({placeholders})
    """
    if ordered:
        query += " order by code"
    return query, list(selection.codes)


def _available_source_columns(
    connection: duckdb.DuckDBPyConnection,
) -> set[str]:
    return {
        row[0]
        for row in connection.execute("describe select * from products").fetchall()
    }


def _require_code_column(
    available_columns: Collection[str],
    *,
    source: Path,
) -> None:
    if "code" not in available_columns:
        raise ValueError(
            f"Source snapshot {source} must expose a products.code column."
        )


def _summarize_duckdb_all_products(
    connection: duckdb.DuckDBPyConnection,
    *,
    source: Path,
) -> SourceInputSummary:
    result = connection.execute(
        """
        select
            count(*) filter (where code is not null and trim(code) != ''),
            count(*) filter (where code is null or trim(code) = '')
        from products
        """
    ).fetchone()
    if result is None:
        raise RuntimeError(
            f"DuckDB source snapshot {source} did not return aggregate row counts."
        )
    processed_product_count = int(result[0] or 0)
    skipped_row_count = int(result[1] or 0)
    skipped_row_examples = tuple(
        SkippedSourceRow(
            location=f"duckdb rowid {int(rowid)}",
            reason=MISSING_OR_BLANK_CODE_REASON,
        )
        for (rowid,) in connection.execute(
            """
            select rowid
            from products
            where code is null
               or trim(code) = ''
            order by rowid
            limit ?
            """,
            [SKIPPED_SOURCE_ROW_EXAMPLE_LIMIT],
        ).fetchall()
    )
    return SourceInputSummary(
        processed_product_count=processed_product_count,
        skipped_row_count=skipped_row_count,
        skipped_row_examples=skipped_row_examples,
    )


def _quote_identifier(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'
