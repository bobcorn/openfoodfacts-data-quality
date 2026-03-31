from __future__ import annotations

import logging

import pytest

from openfoodfacts_data_quality.progress import iter_with_progress


def test_iter_with_progress_logs_periodically(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)

    items = list(range(25))
    consumed = list(
        iter_with_progress(
            items,
            desc="Demo | Step",
            unit="item",
            logger=logging.getLogger("tests.progress"),
        )
    )

    assert consumed == items
    messages = [record.getMessage() for record in caplog.records]
    assert "[Demo | Step] 2/25 items processed." in messages
    assert "[Demo | Step] 24/25 items processed." in messages
    assert "[Demo | Step] 25/25 items processed." in messages
