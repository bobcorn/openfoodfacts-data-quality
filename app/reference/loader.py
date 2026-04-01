from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.legacy_backend.input_projection import build_legacy_backend_input_products
from app.run.models import ResolvedReferenceResults

if TYPE_CHECKING:
    from app.run.models import (
        SupportsLegacyBackendRunner,
        SupportsReferenceResultCache,
    )
    from openfoodfacts_data_quality.contracts.raw import RawProductRow


@dataclass(frozen=True, slots=True)
class ReferenceResultLoader:
    """Load ordered reference results through cache and backend materialization."""

    legacy_backend_runner: SupportsLegacyBackendRunner
    reference_result_cache: SupportsReferenceResultCache

    def load_many(
        self,
        rows: list[RawProductRow],
    ) -> ResolvedReferenceResults:
        """Return one ordered reference result list matching the requested input batch."""
        if not rows:
            return ResolvedReferenceResults(
                reference_results=[],
                cache_hit_count=0,
                backend_run_count=0,
            )

        cached_results = self.reference_result_cache.load_many(
            [row.code for row in rows]
        )
        missing_rows = [row for row in rows if row.code not in cached_results]
        missing_backend_input_products = (
            build_legacy_backend_input_products(missing_rows) if missing_rows else []
        )
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
            reference_results=[results_by_code[row.code] for row in rows],
            cache_hit_count=len(cached_results),
            backend_run_count=len(missing_backend_input_products),
        )
