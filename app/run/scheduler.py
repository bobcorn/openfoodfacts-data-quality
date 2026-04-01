from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, wait
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from app.run.models import (
    BatchExecutionResult,
    ScheduledBatch,
    SupportsBatchExecutor,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

ProcessBatch = Callable[[ScheduledBatch], BatchExecutionResult]


def _in_flight_batches() -> dict[Future[BatchExecutionResult], int]:
    """Return the typed future-to-batch-index map used by the scheduler."""
    return {}


def _completed_batches() -> dict[int, BatchExecutionResult]:
    """Return the typed merge buffer used by the scheduler."""
    return {}


@dataclass
class BatchScheduler:
    """Own the in-flight batch queue and ordered merge bookkeeping."""

    batch_iterator: Iterator[ScheduledBatch]
    executor: SupportsBatchExecutor
    process_batch: ProcessBatch
    worker_limit: int
    in_flight: dict[Future[BatchExecutionResult], int] = field(
        default_factory=_in_flight_batches
    )
    completed_by_index: dict[int, BatchExecutionResult] = field(
        default_factory=_completed_batches
    )
    next_batch_to_merge: int = 1
    exhausted_batches: bool = False

    def has_pending_work(self) -> bool:
        """Return whether any batch is left to submit or merge."""
        return bool(self.in_flight) or not self.exhausted_batches

    def submit_ready_batches(self) -> None:
        """Submit more work until the worker budget is saturated."""
        while not self.exhausted_batches and len(self.in_flight) < self.worker_limit:
            try:
                batch = next(self.batch_iterator)
            except StopIteration:
                self.exhausted_batches = True
                return
            future = self.executor.submit(self.process_batch, batch)
            self.in_flight[future] = batch.batch_index

    def wait_for_completed_batches(
        self, timeout_seconds: float
    ) -> set[Future[BatchExecutionResult]]:
        """Return futures that completed within the timeout budget."""
        if not self.in_flight:
            return set()
        done, _ = wait(
            self.in_flight,
            return_when=FIRST_COMPLETED,
            timeout=timeout_seconds,
        )
        return done

    def record_completed_batches(
        self,
        completed_futures: set[Future[BatchExecutionResult]],
    ) -> None:
        """Move finished futures from the executor queue into the merge buffer."""
        for future in completed_futures:
            batch_index = self.in_flight.pop(future)
            self.completed_by_index[batch_index] = future.result()

    def merge_ready_batches(self) -> list[BatchExecutionResult]:
        """Return newly contiguous batch results in merge order."""
        merged_results: list[BatchExecutionResult] = []
        while self.next_batch_to_merge in self.completed_by_index:
            merged_results.append(self.completed_by_index.pop(self.next_batch_to_merge))
            self.next_batch_to_merge += 1
        return merged_results

    def buffered_results(self) -> tuple[BatchExecutionResult, ...]:
        """Return buffered completed results that are waiting on earlier batches."""
        return tuple(self.completed_by_index.values())

    def merged_batch_count(self) -> int:
        """Return the number of already merged batches."""
        return self.next_batch_to_merge - 1
