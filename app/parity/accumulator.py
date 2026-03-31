from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from app.parity.models import (
    CheckParityResult,
    ObservedFinding,
    ParityResult,
    ParityRunMetadata,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from app.parity.models import ComparisonStatus
    from openfoodfacts_data_quality.contracts.checks import CheckDefinition


def _finding_examples() -> list[ObservedFinding]:
    """Return a typed list used for retained mismatch examples."""
    return []


@dataclass
class _CheckAccumulator:
    """Accumulate exact parity counts while retaining only capped mismatch examples."""

    definition: CheckDefinition
    comparison_status: ComparisonStatus | None = None
    reference_count: int = 0
    migrated_count: int = 0
    matched_count: int = 0
    missing_count: int = 0
    extra_count: int = 0
    missing_examples: list[ObservedFinding] = field(default_factory=_finding_examples)
    extra_examples: list[ObservedFinding] = field(default_factory=_finding_examples)

    def add_batch(self, batch: CheckParityResult, max_examples_per_side: int) -> None:
        """Merge one batch-level check result into the accumulated totals."""
        if self.comparison_status is None:
            self.comparison_status = batch.comparison_status
        elif self.comparison_status != batch.comparison_status:
            raise ValueError(
                f"Inconsistent comparison status for check {self.definition.id}: "
                f"{self.comparison_status!r} vs {batch.comparison_status!r}"
            )
        self.reference_count += batch.reference_count
        self.migrated_count += batch.migrated_count
        self.matched_count += batch.matched_count
        self.missing_count += batch.missing_count
        self.extra_count += batch.extra_count
        self._extend_examples(
            self.missing_examples, batch.missing, max_examples_per_side
        )
        self._extend_examples(self.extra_examples, batch.extra, max_examples_per_side)

    def build_result(self) -> CheckParityResult:
        """Build the final summary-first parity result for one check."""
        comparison_status = self.comparison_status or "compared"
        return CheckParityResult(
            definition=self.definition,
            comparison_status=comparison_status,
            reference_count=self.reference_count,
            migrated_count=self.migrated_count,
            matched_count=self.matched_count,
            missing_count=self.missing_count,
            extra_count=self.extra_count,
            missing=self.missing_examples,
            extra=self.extra_examples,
            passed=(
                self.missing_count == 0 and self.extra_count == 0
                if comparison_status == "compared"
                else None
            ),
        )

    @staticmethod
    def _extend_examples(
        destination: list[ObservedFinding],
        source: list[ObservedFinding],
        limit: int,
    ) -> None:
        """Append only as many examples as the configured retention budget allows."""
        remaining = limit - len(destination)
        if remaining <= 0:
            return
        destination.extend(source[:remaining])


class ParityAccumulator:
    """Aggregate batch-level parity results into one report-sized summary."""

    def __init__(
        self,
        *,
        max_examples_per_side: int,
        checks: Sequence[CheckDefinition],
    ) -> None:
        self._active_checks = tuple(checks)
        self._checks = {
            check.id: _CheckAccumulator(definition=check)
            for check in self._active_checks
        }
        self._max_examples_per_side = max_examples_per_side
        self._reference_total = 0
        self._migrated_total = 0
        self._matched_total = 0
        self._not_compared_migrated_total = 0

    def add_batch(self, batch_result: ParityResult) -> None:
        """Merge one batch parity result into the accumulated run summary."""
        self._reference_total += batch_result.reference_total
        self._migrated_total += batch_result.migrated_total
        self._matched_total += batch_result.matched_total
        self._not_compared_migrated_total += batch_result.not_compared_migrated_total

        for batch_check in batch_result.checks:
            self._checks[batch_check.definition.id].add_batch(
                batch_check,
                max_examples_per_side=self._max_examples_per_side,
            )

    def build_result(
        self,
        *,
        run: ParityRunMetadata,
    ) -> ParityResult:
        """Materialize the final accumulated parity result."""
        return ParityResult(
            run_id=run.run_id,
            source_snapshot_id=run.source_snapshot_id,
            product_count=run.product_count,
            checks=[
                self._checks[check.id].build_result() for check in self._active_checks
            ],
            compared_check_count=sum(
                1 for check in self._active_checks if check.parity_baseline == "legacy"
            ),
            not_compared_check_count=sum(
                1 for check in self._active_checks if check.parity_baseline == "none"
            ),
            reference_total=self._reference_total,
            migrated_total=self._migrated_total,
            matched_total=self._matched_total,
            not_compared_migrated_total=self._not_compared_migrated_total,
        )
