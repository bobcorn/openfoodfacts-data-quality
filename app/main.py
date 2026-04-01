from __future__ import annotations

import logging
import os
from pathlib import Path

from app.reference.cache import configured_reference_result_cache_dir
from app.report.preview import serve
from app.run import (
    build_site,
    configured_batch_size,
    configured_batch_workers,
    configured_database_path,
    configured_legacy_backend_workers,
    configured_mismatch_examples_limit,
)

DEFAULT_PORT = 8000
LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    """Configure the CLI logger used by the local preview runner."""
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)


def configured_port() -> int:
    """Return the configured local preview port."""
    configured = os.environ.get("PORT")
    if not configured:
        return DEFAULT_PORT
    return int(configured)


def main() -> None:
    """Build artifacts, render the report, and preview it locally."""
    configure_logging()
    project_root = Path(__file__).resolve().parents[1]
    db_path = configured_database_path(project_root)
    port = configured_port()
    batch_size = configured_batch_size()
    batch_workers = configured_batch_workers()
    mismatch_examples_limit = configured_mismatch_examples_limit()
    legacy_backend_workers = configured_legacy_backend_workers()
    reference_result_cache_dir = configured_reference_result_cache_dir(project_root)
    LOGGER.info("[Config] Source DB: %s", db_path)
    LOGGER.info("[Config] Batch size: %d", batch_size)
    LOGGER.info("[Config] Concurrent batch workers: %d", batch_workers)
    LOGGER.info(
        "[Config] Persistent legacy backend workers: %d", legacy_backend_workers
    )
    LOGGER.info(
        "[Config] Mismatch example cap per side/check: %d", mismatch_examples_limit
    )
    LOGGER.info(
        "[Config] Reference result cache dir: %s",
        reference_result_cache_dir,
    )
    site_dir = build_site(
        project_root,
        db_path=db_path,
        batch_size=batch_size,
        mismatch_examples_limit=mismatch_examples_limit,
        batch_workers=batch_workers,
        legacy_backend_workers=legacy_backend_workers,
    )
    url = f"http://localhost:{port}/"
    LOGGER.info("[Preview] Report ready.")
    LOGGER.info("[Preview] Open %s to view the report.", url)
    serve(site_dir, port)


if __name__ == "__main__":
    main()
