from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import logging
    from collections.abc import Iterator, Sequence


def iter_with_progress[T](
    items: Sequence[T],
    *,
    desc: str,
    unit: str,
    logger: logging.Logger,
) -> Iterator[T]:
    """Iterate with periodic progress logs in non-interactive app flows."""
    total = len(items)
    if total == 0:
        logger.info("[%s] 0/%d %ss processed.", desc, total, unit)
        return

    step = 1 if total <= 20 else max(1, total // 10)
    for index, item in enumerate(items, start=1):
        yield item
        if index % step == 0 or index == total:
            logger.info("[%s] %d/%d %ss processed.", desc, index, total, unit)
