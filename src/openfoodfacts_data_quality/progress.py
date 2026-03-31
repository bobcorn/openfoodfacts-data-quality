from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    import logging
    from collections.abc import Iterator, Sequence

T = TypeVar("T")


def iter_with_progress(
    items: Sequence[T],
    *,
    desc: str,
    unit: str,
    logger: logging.Logger,
) -> Iterator[T]:
    """Iterate with periodic progress logs in every runtime environment."""
    total = len(items)
    if total == 0:
        logger.info("[%s] 0/%d %ss processed.", desc, total, unit)
        return

    step = _log_step_for(total)
    for index, item in enumerate(items, start=1):
        yield item
        if index % step == 0 or index == total:
            logger.info("[%s] %d/%d %ss processed.", desc, index, total, unit)


def _log_step_for(total: int) -> int:
    """Choose a coarse non-TTY logging cadence."""
    if total <= 20:
        return 1
    return max(1, total // 10)
