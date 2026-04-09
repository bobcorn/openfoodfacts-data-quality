from __future__ import annotations

from collections.abc import Sequence

from app.source.models import ProductDocument


def build_legacy_backend_input_payloads(
    product_documents: Sequence[ProductDocument],
) -> list[dict[str, object]]:
    """Return serialized product document payloads for the legacy backend."""
    return [
        product_document.backend_input_payload()
        for product_document in product_documents
    ]


__all__ = [
    "build_legacy_backend_input_payloads",
]
