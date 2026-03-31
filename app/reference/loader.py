from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.pipeline.models import ResolvedReferenceResults

if TYPE_CHECKING:
    from app.legacy_backend.input_projection import LegacyBackendInputProduct
    from app.pipeline.models import (
        SupportsLegacyBackendRunner,
        SupportsReferenceResultCache,
    )


@dataclass(frozen=True, slots=True)
class ReferenceResultLoader:
    """Load ordered reference results through cache and backend materialization."""

    legacy_backend_runner: SupportsLegacyBackendRunner
    reference_result_cache: SupportsReferenceResultCache

    def load_many(
        self,
        backend_input_products: list[LegacyBackendInputProduct],
    ) -> ResolvedReferenceResults:
        """Return one ordered reference-result list matching the requested input batch."""
        if not backend_input_products:
            return ResolvedReferenceResults(
                reference_results=[],
                cache_hit_count=0,
                backend_run_count=0,
            )

        cached_results = self.reference_result_cache.load_many(
            [product.code for product in backend_input_products]
        )
        missing_backend_input_products = [
            product
            for product in backend_input_products
            if product.code not in cached_results
        ]
        fresh_results = (
            self.legacy_backend_runner.run(missing_backend_input_products)
            if missing_backend_input_products
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
                results_by_code[product.code] for product in backend_input_products
            ],
            cache_hit_count=len(cached_results),
            backend_run_count=len(missing_backend_input_products),
        )
