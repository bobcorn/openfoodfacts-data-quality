from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

type Severity = Literal["info", "completeness", "warning", "error", "bug"]

VALIDATION_HEADERS: tuple[str, ...] = (
    "dq_status",
    "dq_severity",
    "dq_findings_count",
    "dq_findings",
    "dq_last_checked_at",
)
VALIDATION_HEADER_SET = frozenset(VALIDATION_HEADERS)


@dataclass(frozen=True)
class TableData:
    headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]

    def column_index(self, column_name: str) -> int | None:
        try:
            return self.headers.index(column_name)
        except ValueError:
            return None


@dataclass(frozen=True)
class ValidationOutcome:
    table: TableData
    row_backgrounds: dict[int, str]
    validated_rows: int
    issue_rows: int
    error_rows: int


@dataclass(frozen=True)
class MockUploadPreview:
    candidate_count: int
    candidate_codes: tuple[str, ...]
    sample_payloads: tuple[dict[str, str], ...]
