from __future__ import annotations

from collections.abc import Callable

import pytest
from app.legacy_backend.input_projection import LegacyBackendInputProduct
from app.reference.loader import ReferenceResultLoader
from app.reference.models import ReferenceResult

from openfoodfacts_data_quality.contracts.raw import RawProductRow

ReferenceResultFactory = Callable[..., ReferenceResult]


class _RecordingLegacyBackendRunner:
    def __init__(self, fresh_results: list[ReferenceResult]) -> None:
        self.fresh_results = fresh_results
        self.calls: list[list[str]] = []

    def run(
        self,
        backend_input_products: list[LegacyBackendInputProduct],
    ) -> list[ReferenceResult]:
        self.calls.append([product.code for product in backend_input_products])
        return self.fresh_results


class _RecordingReferenceResultCache:
    def __init__(self, cached_results: dict[str, ReferenceResult]) -> None:
        self.cached_results = cached_results
        self.load_calls: list[list[str]] = []
        self.store_calls: list[list[str]] = []

    def load_many(self, codes: list[str]) -> dict[str, ReferenceResult]:
        self.load_calls.append(codes)
        return {
            code: self.cached_results[code]
            for code in codes
            if code in self.cached_results
        }

    def store_many(self, reference_results: list[ReferenceResult]) -> None:
        self.store_calls.append([result.code for result in reference_results])


def _raw_row(code: str) -> RawProductRow:
    return RawProductRow.model_validate({"code": code})


def test_reference_result_loader_uses_cache_without_backend_projection(
    monkeypatch: pytest.MonkeyPatch,
    reference_result_factory: ReferenceResultFactory,
) -> None:
    cached_result = reference_result_factory(code="123")
    cache = _RecordingReferenceResultCache({"123": cached_result})
    loader = ReferenceResultLoader(
        legacy_backend_runner=_RecordingLegacyBackendRunner([]),
        reference_result_cache=cache,
    )

    def fail_build_input_products(_: object) -> list[object]:
        raise AssertionError("Backend projection should not run on a full cache hit.")

    monkeypatch.setattr(
        "app.reference.loader.build_legacy_backend_input_products",
        fail_build_input_products,
    )

    resolved = loader.load_many([_raw_row("123")])

    assert cache.load_calls == [["123"]]
    assert cache.store_calls == []
    assert resolved.cache_hit_count == 1
    assert resolved.backend_run_count == 0
    assert [result.code for result in resolved.reference_results] == ["123"]


def test_reference_result_loader_projects_only_cache_misses(
    reference_result_factory: ReferenceResultFactory,
) -> None:
    cached_result = reference_result_factory(code="123")
    fresh_result = reference_result_factory(code="456")
    cache = _RecordingReferenceResultCache({"123": cached_result})
    backend_runner = _RecordingLegacyBackendRunner([fresh_result])
    loader = ReferenceResultLoader(
        legacy_backend_runner=backend_runner,
        reference_result_cache=cache,
    )

    resolved = loader.load_many([_raw_row("123"), _raw_row("456")])

    assert cache.load_calls == [["123", "456"]]
    assert cache.store_calls == [["456"]]
    assert backend_runner.calls == [["456"]]
    assert resolved.cache_hit_count == 1
    assert resolved.backend_run_count == 1
    assert [result.code for result in resolved.reference_results] == ["123", "456"]
