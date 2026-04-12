from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_MODULE_PATH = SCRIPTS_DIR / "refresh_example_data.py"
_SPEC = importlib.util.spec_from_file_location("refresh_example_data", _MODULE_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Unable to load {_MODULE_PATH}.")
refresh_example_data = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(refresh_example_data)


def test_write_sample_csv_preserves_original_header_and_row_text(
    tmp_path: Path,
) -> None:
    source_csv = tmp_path / "official.tsv"
    source_csv.write_text(
        "code\tproduct_name\tquantity\n"
        '001\t"Alpha, First"\t100 g\n'
        "002\tBeta\t200 g\n"
        "003\tGamma\t300 g\n",
        encoding="utf-8",
    )
    output_csv = tmp_path / "sample.tsv"

    count = refresh_example_data._write_sample_csv(
        source_csv=source_csv,
        output_csv=output_csv,
        selected_codes=("003", "001"),
    )

    assert count == 2
    assert output_csv.read_text(encoding="utf-8") == (
        'code\tproduct_name\tquantity\n003\tGamma\t300 g\n001\t"Alpha, First"\t100 g\n'
    )


def test_write_sample_parquet_preserves_schema_and_selected_codes(
    tmp_path: Path,
) -> None:
    source_parquet = tmp_path / "official.parquet"
    output_parquet = tmp_path / "sample.parquet"

    connection = duckdb.connect()
    try:
        connection.execute(
            f"""
            copy (
                select *
                from (
                    values
                        (
                            '001',
                            1,
                            [
                                {{'lang': 'main', 'text': 'Alpha'}},
                                {{'lang': 'en', 'text': 'Alpha'}}
                            ]::struct(lang varchar, "text" varchar)[],
                            [
                                {{
                                    'name': 'energy-kcal',
                                    'value': null,
                                    '100g': 10.0,
                                    'serving': null,
                                    'unit': 'kcal',
                                    'prepared_value': null,
                                    'prepared_100g': null,
                                    'prepared_serving': null,
                                    'prepared_unit': null
                                }}
                            ]::struct(
                                "name" varchar,
                                "value" double,
                                "100g" double,
                                serving double,
                                unit varchar,
                                prepared_value double,
                                prepared_100g double,
                                prepared_serving double,
                                prepared_unit varchar
                            )[]
                        ),
                        (
                            '002',
                            2,
                            [
                                {{'lang': 'main', 'text': 'Beta'}}
                            ]::struct(lang varchar, "text" varchar)[],
                            [
                                {{
                                    'name': 'energy-kcal',
                                    'value': null,
                                    '100g': 20.0,
                                    'serving': null,
                                    'unit': 'kcal',
                                    'prepared_value': null,
                                    'prepared_100g': null,
                                    'prepared_serving': null,
                                    'prepared_unit': null
                                }}
                            ]::struct(
                                "name" varchar,
                                "value" double,
                                "100g" double,
                                serving double,
                                unit varchar,
                                prepared_value double,
                                prepared_100g double,
                                prepared_serving double,
                                prepared_unit varchar
                            )[]
                        )
                ) as t(code, created_t, product_name, nutriments)
            ) to '{source_parquet}' (format parquet)
            """
        )
    finally:
        connection.close()

    count = refresh_example_data._write_sample_parquet(
        source_parquet=source_parquet,
        output_parquet=output_parquet,
        selected_codes=("002", "001"),
    )

    source_connection = duckdb.connect()
    try:
        source_schema = source_connection.sql(
            f"describe select * from read_parquet('{source_parquet}')"
        ).fetchall()
        output_schema = source_connection.sql(
            f"describe select * from read_parquet('{output_parquet}')"
        ).fetchall()
        selected_codes = source_connection.sql(
            f"select code from read_parquet('{output_parquet}')"
        ).fetchall()
    finally:
        source_connection.close()

    assert count == 2
    assert output_schema == source_schema
    assert selected_codes == [("002",), ("001",)]


def test_write_sample_duckdb_from_jsonl_preserves_import_schema(tmp_path: Path) -> None:
    sample_jsonl = tmp_path / "products.jsonl"
    sample_jsonl.write_text(
        "\n".join(
            [
                '{"code":"001","product_quantity":"28","serving_quantity":28.5,"product_name":"Alpha"}',
                '{"code":"002","product_quantity":"15","serving_quantity":15,"product_name":"Beta"}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    output_duckdb = tmp_path / "products-jsonl.duckdb"

    refresh_example_data._write_sample_duckdb_from_jsonl(sample_jsonl, output_duckdb)

    imported_schema = _describe_duckdb_products(output_duckdb)
    reference_schema = _describe_query(
        "describe select * from read_json_auto("
        f"'{sample_jsonl}', format='newline_delimited'"
        ")"
    )

    assert imported_schema == reference_schema


def test_write_sample_duckdb_from_parquet_preserves_import_schema(
    tmp_path: Path,
) -> None:
    source_parquet = tmp_path / "products.parquet"
    output_duckdb = tmp_path / "products-parquet.duckdb"

    connection = duckdb.connect()
    try:
        connection.execute(
            f"""
            copy (
                select *
                from (
                    values
                        ('001', [{{'lang': 'main', 'text': 'Alpha'}}]::struct(lang varchar, "text" varchar)[]),
                        ('002', [{{'lang': 'main', 'text': 'Beta'}}]::struct(lang varchar, "text" varchar)[])
                ) as t(code, product_name)
            ) to '{source_parquet}' (format parquet)
            """
        )
    finally:
        connection.close()

    refresh_example_data._write_sample_duckdb_from_parquet(
        source_parquet, output_duckdb
    )

    imported_schema = _describe_duckdb_products(output_duckdb)
    reference_schema = _describe_query(
        f"describe select * from read_parquet('{source_parquet}')"
    )

    assert imported_schema == reference_schema


def _describe_duckdb_products(path: Path) -> list[tuple[object, ...]]:
    connection = duckdb.connect(str(path), read_only=True)
    try:
        return connection.sql("describe select * from products").fetchall()
    finally:
        connection.close()


def _describe_query(query: str) -> list[tuple[object, ...]]:
    connection = duckdb.connect()
    try:
        return connection.sql(query).fetchall()
    finally:
        connection.close()
