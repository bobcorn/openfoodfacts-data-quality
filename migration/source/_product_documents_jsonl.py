from __future__ import annotations

import gzip
import heapq
import json
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import cast

from migration.source._product_documents_common import (
    record_skipped_source_row,
    required_selection_sample_size,
    required_selection_seed,
    skip_reason_from_product_document_error,
    stable_sample_hash,
)
from migration.source.datasets import SourceSelection
from migration.source.models import (
    ProductDocument,
    SkippedSourceRow,
    SourceBatchRecord,
    SourceInputSummary,
)
from migration.source.product_documents import (
    source_batch_record_from_document,
    validate_product_document,
)


class JsonlProductDocumentAdapter:
    """Read full product documents from one JSON Lines source snapshot."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def summarize_input(
        self,
        *,
        selection: SourceSelection | None = None,
    ) -> SourceInputSummary:
        """Return source input diagnostics for the selected JSONL rows."""
        if selection is None or selection.kind == "all_products":
            return _summarize_jsonl_all_products(self.path)
        if selection.kind == "code_list":
            return SourceInputSummary(
                processed_product_count=len(
                    _selected_jsonl_code_list_documents(self.path, selection.codes)
                )
            )
        _, summary = _selected_jsonl_stable_sample_result(
            self.path,
            sample_size=required_selection_sample_size(selection),
            seed=required_selection_seed(selection),
        )
        return summary

    def iter_batches(
        self,
        *,
        batch_size: int,
        selection: SourceSelection | None = None,
    ) -> Iterator[list[SourceBatchRecord]]:
        """Yield JSONL product documents as source batch records."""
        if batch_size <= 0:
            raise ValueError("batch_size must be a positive integer.")

        batch: list[SourceBatchRecord] = []
        for document in self._iter_selected_documents(selection=selection):
            batch.append(source_batch_record_from_document(document))
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _iter_selected_documents(
        self,
        *,
        selection: SourceSelection | None,
    ) -> Iterator[ProductDocument]:
        if selection is None or selection.kind == "all_products":
            return _iter_valid_product_documents(self.path)
        if selection.kind == "code_list":
            return iter(
                _selected_jsonl_code_list_documents(
                    self.path,
                    selection.codes,
                )
            )
        documents, _ = _selected_jsonl_stable_sample_result(
            self.path,
            sample_size=required_selection_sample_size(selection),
            seed=required_selection_seed(selection),
        )
        return iter(documents)


def _iter_nonblank_jsonl_lines(path: Path) -> Iterator[tuple[int, str]]:
    if tuple(suffix.lower() for suffix in path.suffixes[-2:]) == (".jsonl", ".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if line.strip():
                    yield line_number, line
        return

    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if line.strip():
                yield line_number, line


def _iter_jsonl_documents(path: Path) -> Iterator[tuple[int, dict[str, object]]]:
    for line_number, line in _iter_nonblank_jsonl_lines(path):
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(
                f"JSONL source line {line_number} is not a product object."
            )
        yield line_number, cast(dict[str, object], payload)


def _iter_valid_product_documents(path: Path) -> Iterator[ProductDocument]:
    for line_number, document in _iter_jsonl_documents(path):
        product_document, skipped_row = _validated_jsonl_product_document(
            document,
            line_number=line_number,
        )
        if skipped_row is None:
            assert product_document is not None
            yield product_document


def _selected_jsonl_code_list_documents(
    path: Path,
    codes: Sequence[str],
) -> list[ProductDocument]:
    selected_codes = set(codes)
    documents = [
        validate_product_document(document)
        for _, document in _iter_jsonl_documents(path)
        if _raw_product_code(document) in selected_codes
    ]
    return sorted(documents, key=lambda document: document.code)


def _selected_jsonl_stable_sample_result(
    path: Path,
    *,
    sample_size: int,
    seed: int,
) -> tuple[list[ProductDocument], SourceInputSummary]:
    heap: list[tuple[int, str, int, ProductDocument]] = []
    skipped_row_count = 0
    skipped_row_examples: list[SkippedSourceRow] = []
    for line_number, document in _iter_jsonl_documents(path):
        product_document, skipped_row = _validated_jsonl_product_document(
            document,
            line_number=line_number,
        )
        if skipped_row is not None:
            skipped_row_count = record_skipped_source_row(
                skipped_row,
                skipped_row_count=skipped_row_count,
                skipped_row_examples=skipped_row_examples,
            )
            continue
        assert product_document is not None
        code = product_document.code
        stable_hash = stable_sample_hash(code, seed)
        entry = (-stable_hash, code, line_number, product_document)
        if len(heap) < sample_size:
            heapq.heappush(heap, entry)
            continue
        if entry > heap[0]:
            heapq.heapreplace(heap, entry)
    documents = [
        document
        for _, _, _, document in sorted(
            heap,
            key=lambda entry: (-entry[0], entry[1], entry[2]),
        )
    ]
    return (
        documents,
        SourceInputSummary(
            processed_product_count=len(documents),
            skipped_row_count=skipped_row_count,
            skipped_row_examples=tuple(skipped_row_examples),
        ),
    )


def _summarize_jsonl_all_products(path: Path) -> SourceInputSummary:
    processed_product_count = 0
    skipped_row_count = 0
    skipped_row_examples: list[SkippedSourceRow] = []
    for line_number, document in _iter_jsonl_documents(path):
        _, skipped_row = _validated_jsonl_product_document(
            document,
            line_number=line_number,
        )
        if skipped_row is not None:
            skipped_row_count = record_skipped_source_row(
                skipped_row,
                skipped_row_count=skipped_row_count,
                skipped_row_examples=skipped_row_examples,
            )
            continue
        processed_product_count += 1
    return SourceInputSummary(
        processed_product_count=processed_product_count,
        skipped_row_count=skipped_row_count,
        skipped_row_examples=tuple(skipped_row_examples),
    )


def _validated_jsonl_product_document(
    document: dict[str, object],
    *,
    line_number: int,
) -> tuple[ProductDocument | None, SkippedSourceRow | None]:
    try:
        return validate_product_document(document), None
    except ValueError as exc:
        reason = skip_reason_from_product_document_error(exc)
        if reason is None:
            raise
        return (
            None,
            SkippedSourceRow(
                location=f"jsonl line {line_number}",
                reason=reason,
            ),
        )


def _raw_product_code(document: Mapping[str, object]) -> str | None:
    value = document.get("code")
    if not isinstance(value, str):
        return None
    code = value.strip()
    return code or None
