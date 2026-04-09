from __future__ import annotations

import logging
from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterable

    from app.run.models import BatchStageTimings

LOGGER = logging.getLogger(__name__)
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 15.0


class SupportsRowCount(Protocol):
    """Minimal shape needed to count completed products."""

    @property
    def row_count(self) -> int: ...


class SupportsBatchLog(Protocol):
    """Minimal shape needed to log a completed batch."""

    @property
    def batch_index(self) -> int: ...

    @property
    def cache_hit_count(self) -> int: ...

    @property
    def backend_run_count(self) -> int: ...

    @property
    def reference_finding_count(self) -> int: ...

    @property
    def migrated_finding_count(self) -> int: ...

    @property
    def row_count(self) -> int: ...

    @property
    def elapsed_seconds(self) -> float: ...

    @property
    def stage_timings(self) -> BatchStageTimings: ...


@dataclass(frozen=True)
class ExecutionPlan:
    """Execution sizing derived from the current snapshot and batch configuration."""

    product_count: int
    batch_size: int
    batch_count: int
    configured_workers: int
    effective_workers: int


@dataclass(frozen=True)
class ExecutionProgressConfig:
    """Static execution metadata needed by progress reporting."""

    plan: ExecutionPlan
    mismatch_examples_limit: int


def build_execution_plan(
    *,
    product_count: int,
    batch_size: int,
    configured_workers: int,
) -> ExecutionPlan:
    """Return the effective batch and worker sizing for the current execution."""
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")
    if configured_workers <= 0:
        raise ValueError("configured_workers must be a positive integer.")

    batch_count = (
        0 if product_count <= 0 else (product_count + batch_size - 1) // batch_size
    )
    effective_workers = min(configured_workers, batch_count) if batch_count else 0
    return ExecutionPlan(
        product_count=product_count,
        batch_size=batch_size,
        batch_count=batch_count,
        configured_workers=configured_workers,
        effective_workers=effective_workers,
    )


def finished_execution_progress(
    *,
    processed_products: int,
    buffered_results: Iterable[SupportsRowCount],
    merged_batch_count: int,
) -> tuple[int, int]:
    """Return finished product and batch counts, including buffered completions."""
    buffered_results = tuple(buffered_results)
    finished_products = processed_products + sum(
        result.row_count for result in buffered_results
    )
    finished_batches = merged_batch_count + len(buffered_results)
    return finished_products, finished_batches


class ExecutionProgressReporter:
    """Own the formatting and cadence of execution-progress logs."""

    def __init__(
        self,
        *,
        config: ExecutionProgressConfig,
        logger: logging.Logger = LOGGER,
        started_at: float | None = None,
    ) -> None:
        self._config = config
        self._logger = logger
        self._started_at = perf_counter() if started_at is None else started_at

    @property
    def heartbeat_interval_seconds(self) -> float:
        """Return the maximum silent interval before emitting a heartbeat."""
        return DEFAULT_HEARTBEAT_INTERVAL_SECONDS

    def log_plan(self) -> None:
        """Log the planned execution shape for the current run."""
        self._logger.info(
            "[Execution] Planned %d products across %d batch(es); effective batch workers: %d/%d.",
            self._config.plan.product_count,
            self._config.plan.batch_count,
            self._config.plan.effective_workers,
            self._config.plan.configured_workers,
        )
        self._logger.info(
            "[Execution] Running checks in batches of %d products (keeping up to %d mismatch examples per side and check).",
            self._config.plan.batch_size,
            self._config.mismatch_examples_limit,
        )

    def log_heartbeat(
        self,
        *,
        processed_products: int,
        buffered_results: Iterable[SupportsRowCount],
        merged_batch_count: int,
        in_flight_count: int,
    ) -> None:
        """Log a compact heartbeat when no batch has completed for a while."""
        finished_products, finished_batches = finished_execution_progress(
            processed_products=processed_products,
            buffered_results=buffered_results,
            merged_batch_count=merged_batch_count,
        )
        self._logger.info(
            "[Execution] Still running after %.1fs: %d / %d products finished, %d / %d batches finished, %d in flight.",
            perf_counter() - self._started_at,
            finished_products,
            self._config.plan.product_count,
            finished_batches,
            self._config.plan.batch_count,
            in_flight_count,
        )

    def log_batch_completed(
        self,
        batch_result: SupportsBatchLog,
        *,
        processed_products: int,
    ) -> None:
        """Log the completion of one merged batch."""
        self._logger.info(
            "[Batch %d] Cache %d hit(s), backend %d product(s), %d reference findings, %d migrated findings, processed %d / %d products in %.1fs (batch %.1fs; stages: source %.1fs, reference load %.1fs, reference contexts %.1fs, reference findings %.1fs, migrated %.1fs, parity %.1fs).",
            batch_result.batch_index,
            batch_result.cache_hit_count,
            batch_result.backend_run_count,
            batch_result.reference_finding_count,
            batch_result.migrated_finding_count,
            processed_products,
            self._config.plan.product_count,
            perf_counter() - self._started_at,
            batch_result.elapsed_seconds,
            batch_result.stage_timings.source_read_seconds,
            batch_result.stage_timings.reference_load_seconds,
            batch_result.stage_timings.reference_check_context_materialization_seconds,
            batch_result.stage_timings.reference_finding_materialization_seconds,
            batch_result.stage_timings.migrated_findings_seconds,
            batch_result.stage_timings.parity_compare_seconds,
        )
