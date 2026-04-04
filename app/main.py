from __future__ import annotations

import logging
from pathlib import Path

from app.application import build_site
from app.report.preview import serve
from app.run import configured_preview_settings, configured_run_spec

LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    """Configure the CLI logger used by the local preview runner."""
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)


def main() -> None:
    """Build artifacts, render the report, and preview it locally."""
    configure_logging()
    project_root = Path(__file__).resolve().parents[1]
    run_spec = configured_run_spec(project_root)
    preview_settings = configured_preview_settings()
    LOGGER.info("[Config] Source DB: %s", run_spec.db_path)
    LOGGER.info("[Config] Batch size: %d", run_spec.batch_size)
    LOGGER.info("[Config] Concurrent batch workers: %d", run_spec.batch_workers)
    LOGGER.info(
        "[Config] Mismatch example cap per side/check: %d",
        run_spec.mismatch_examples_limit,
    )
    LOGGER.info(
        "[Config] Reference result cache dir: %s",
        run_spec.reference_result_cache_dir,
    )
    LOGGER.info(
        "[Config] Persistent legacy backend workers: %d",
        run_spec.legacy_backend_workers,
    )
    LOGGER.info(
        "[Config] Dataset profile: %s",
        run_spec.dataset_profile_name if run_spec.dataset_profile_name else "default",
    )
    LOGGER.info(
        "[Config] Parity store: %s",
        run_spec.parity_store_path
        if run_spec.parity_store_path is not None
        else "disabled",
    )
    LOGGER.info(
        "[Config] Legacy inventory artifact: %s",
        (
            run_spec.legacy_inventory_artifact_path
            if run_spec.legacy_inventory_artifact_path is not None
            else "disabled"
        ),
    )
    LOGGER.info(
        "[Config] Legacy estimation sheet: %s",
        (
            run_spec.legacy_estimation_sheet_path
            if run_spec.legacy_estimation_sheet_path is not None
            else "disabled"
        ),
    )
    site_dir = build_site(run_spec)
    url = f"http://localhost:{preview_settings.port}/"
    LOGGER.info("[Preview] Report ready.")
    LOGGER.info("[Preview] Open %s to view the report.", url)
    serve(site_dir, preview_settings.port)


if __name__ == "__main__":
    main()
