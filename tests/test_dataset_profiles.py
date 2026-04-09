from __future__ import annotations

from pathlib import Path

import duckdb
from app.source.datasets import SourceSelection, load_dataset_profile
from app.source.product_documents import count_source_products, iter_source_batches


def test_load_dataset_profile_supports_stable_sample_and_code_lists(
    tmp_path: Path,
) -> None:
    codes_path = tmp_path / "codes.txt"
    codes_path.write_text(
        """
# comment
0003
0001
0003
""".strip(),
        encoding="utf-8",
    )
    config_path = tmp_path / "dataset-profiles.toml"
    config_path.write_text(
        """
default_profile = "smoke"

[profiles.smoke]
description = "Fast deterministic smoke run."
kind = "stable_sample"
sample_size = 25
seed = 11

[profiles.focus]
description = "Curated code-list validation run."
kind = "code_list"
codes_path = "codes.txt"
""".strip(),
        encoding="utf-8",
    )

    smoke = load_dataset_profile(config_path)
    focus = load_dataset_profile(config_path, "focus")

    assert smoke.selection == SourceSelection(
        kind="stable_sample",
        sample_size=25,
        seed=11,
    )
    assert focus.selection.codes == ("0003", "0001")
    assert focus.selection.codes_path == codes_path.resolve()


def test_code_list_selection_filters_source_rows(
    tmp_path: Path,
) -> None:
    db_path = _write_products_db(tmp_path, ["0001", "0002", "0003"])
    selection = SourceSelection(
        kind="code_list",
        codes=("0003", "0001"),
    )

    count = count_source_products(db_path, selection=selection)
    batches = list(iter_source_batches(db_path, batch_size=2, selection=selection))

    assert count == 2
    assert [[row.source_product.code for row in batch] for batch in batches] == [
        ["0001", "0003"]
    ]


def test_stable_sample_selection_is_repeatable(
    tmp_path: Path,
) -> None:
    db_path = _write_products_db(tmp_path, ["0001", "0002", "0003", "0004"])
    selection = SourceSelection(
        kind="stable_sample",
        sample_size=2,
        seed=7,
    )

    first_codes = [
        row.source_product.code
        for batch in iter_source_batches(db_path, batch_size=10, selection=selection)
        for row in batch
    ]
    second_codes = [
        row.source_product.code
        for batch in iter_source_batches(db_path, batch_size=10, selection=selection)
        for row in batch
    ]

    assert count_source_products(db_path, selection=selection) == 2
    assert len(first_codes) == 2
    assert first_codes == second_codes


def _write_products_db(tmp_path: Path, codes: list[str]) -> Path:
    db_path = tmp_path / "products.duckdb"
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute("create table products (code VARCHAR, product_name VARCHAR)")
        for code in codes:
            connection.execute(
                "insert into products values (?, ?)",
                [code, f"Product {code}"],
            )
        connection.execute("checkpoint")
    finally:
        connection.close()
    return db_path
