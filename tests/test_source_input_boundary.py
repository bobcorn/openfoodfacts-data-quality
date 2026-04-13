from __future__ import annotations

import pytest

from off_data_quality._source_input.codecs import (
    decode_export_localized_text,
    decode_export_nutriment_columns,
    decode_export_tag_values,
    decode_full_document_localized_text,
    decode_full_document_nutriment_columns,
    decode_full_document_tag_values,
)
from off_data_quality._source_input.contracts import SourceInputProbe
from off_data_quality._source_input.dispatch import (
    SUPPORTED_SOURCE_INPUT_CONTRACTS,
    prepare_supported_source_row,
)


def test_source_input_contract_probes_match_only_their_supported_contract() -> None:
    canonical_probe = _contract_probe("canonical_compatible")
    full_document_probe = _contract_probe("off_full_document")
    product_export_probe = _contract_probe("off_product_export")

    canonical_row = {"code": "123", "product_name": "Example"}
    full_document_row = _full_document_row()
    product_export_row = _product_export_row()

    assert canonical_probe(canonical_row, 0).decision == "matched"
    assert full_document_probe(canonical_row, 0).decision == "no_match"
    assert product_export_probe(canonical_row, 0).decision == "no_match"

    assert canonical_probe(full_document_row, 0).decision == "no_match"
    assert full_document_probe(full_document_row, 0).decision == "matched"
    assert product_export_probe(full_document_row, 0).decision == "no_match"

    assert canonical_probe(product_export_row, 0).decision == "no_match"
    assert full_document_probe(product_export_row, 0).decision == "no_match"
    assert product_export_probe(product_export_row, 0).decision == "matched"


def test_source_input_contract_probes_flag_partial_structured_rows_as_invalid() -> None:
    product_export_probe = _contract_probe("off_product_export")
    partial_structured_row: dict[str, object] = {"code": "123", "nutriments": []}

    outcome = product_export_probe(partial_structured_row, 0)

    assert outcome.decision == "invalid"
    assert outcome.reason is not None
    assert "complete official OFF export row" in outcome.reason


def test_prepare_supported_source_row_rejects_partial_structured_rows() -> None:
    with pytest.raises(ValueError, match="complete official OFF export row"):
        prepare_supported_source_row({"code": "123", "nutriments": []}, row_index=0)


def test_source_input_contract_probes_flag_partial_full_document_rows_as_invalid() -> (
    None
):
    full_document_probe = _contract_probe("off_full_document")

    outcome = full_document_probe({"_id": "123", "code": "123"}, 0)

    assert outcome.decision == "invalid"
    assert outcome.reason is not None
    assert "full official JSONL document row" in outcome.reason


def test_full_document_decoders_accept_plain_text_and_mappings() -> None:
    assert decode_full_document_localized_text("Example", column="product_name") == (
        "Example"
    )
    assert (
        decode_full_document_tag_values(
            "en:france",
            column="countries_tags",
        )
        == "en:france"
    )
    assert decode_full_document_nutriment_columns({"energy-kcal_100g": 123.0}) == {
        "energy-kcal_100g": 123.0
    }


def test_product_export_decoders_reject_plain_text_but_accept_structured_values() -> (
    None
):
    with pytest.raises(ValueError, match="localized list"):
        decode_export_localized_text("Example", column="product_name")

    with pytest.raises(ValueError, match="must be a list"):
        decode_export_tag_values("en:france", column="countries_tags")

    assert (
        decode_export_localized_text(
            [{"lang": "main", "text": "Example"}],
            column="product_name",
        )
        == "Example"
    )
    assert decode_export_tag_values(
        ["en:france"],
        column="countries_tags",
    ) == ["en:france"]
    assert decode_export_nutriment_columns(
        [{"name": "energy-kcal", "100g": 123.0}]
    ) == {"energy-kcal_100g": 123.0}


def _contract_probe(contract_id: str) -> SourceInputProbe:
    for contract in SUPPORTED_SOURCE_INPUT_CONTRACTS:
        if contract.id == contract_id:
            return contract.probe
    raise AssertionError(f"Missing supported source input contract: {contract_id}")


def _full_document_row() -> dict[str, object]:
    return {
        "_id": "123",
        "code": "123",
        "product_name": "Example",
        "ingredients_text": "Sugar, salt",
        "ingredients_tags": ["en:sugar", "en:salt"],
        "countries_tags": ["en:france"],
        "images": {},
        "nutriments": {"energy-kcal_100g": 123.0},
    }


def _product_export_row() -> dict[str, object]:
    row: dict[str, object] = {
        "code": "123",
        "created_t": 123,
        "quantity": "500 g",
        "product_quantity": "500",
        "serving_size": "50 g",
        "serving_quantity": "50",
        "brands": "Brand",
        "categories": "Supplements",
        "labels": "No gluten",
        "emb_codes": "FR 01.001",
        "nutriscore_grade": "a",
        "nutriscore_score": -2,
        "no_nutrition_data": False,
        "product_name": [{"lang": "main", "text": "Example"}],
        "ingredients_text": [{"lang": "main", "text": "Sugar, salt"}],
        "ingredients_tags": ["en:sugar", "en:salt"],
        "categories_tags": ["en:supplements"],
        "labels_tags": ["en:vegan"],
        "countries_tags": ["en:france"],
        "nutriments": [{"name": "energy-kcal", "100g": 123.0}],
    }
    return row
