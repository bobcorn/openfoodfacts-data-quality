from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING

from app.legacy_backend.input_payloads import build_legacy_backend_input_payloads
from app.run.models import ResolvedReferenceResults

if TYPE_CHECKING:
    from app.run.models import (
        SupportsLegacyBackendRunner,
        SupportsReferenceResultCache,
    )
    from app.source.models import ProductDocument


@dataclass(frozen=True, slots=True)
class ReferenceResultLoader:
    """Load ordered reference results through cache and backend materialization."""

    legacy_backend_runner: SupportsLegacyBackendRunner
    reference_result_cache: SupportsReferenceResultCache

    def load_many(
        self,
        product_documents: list[ProductDocument],
    ) -> ResolvedReferenceResults:
        """Return one ordered reference result list matching the requested input batch."""
        if not product_documents:
            return ResolvedReferenceResults(
                reference_results=[],
                cache_hit_count=0,
                backend_run_count=0,
                load_seconds=0.0,
            )

        started = perf_counter()
        cached_results = self.reference_result_cache.load_many(
            [document.code for document in product_documents]
        )
        missing_documents = [
            document
            for document in product_documents
            if document.code not in cached_results
        ]
        missing_backend_input_payloads = (
            build_legacy_backend_input_payloads(missing_documents)
            if missing_documents
            else []
        )
        fresh_results = (
            self.legacy_backend_runner.run(missing_backend_input_payloads)
            if missing_backend_input_payloads
            else []
        )
        if fresh_results:
            self.reference_result_cache.store_many(fresh_results)
        results_by_code = {
            **cached_results,
            **{result.code: result for result in fresh_results},
        }
        return ResolvedReferenceResults(
            reference_results=[
                results_by_code[document.code] for document in product_documents
            ],
            cache_hit_count=len(cached_results),
            backend_run_count=len(missing_backend_input_payloads),
            load_seconds=perf_counter() - started,
        )
