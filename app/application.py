from __future__ import annotations

import logging
from pathlib import Path
from time import perf_counter

from app.artifacts import display_path
from app.report.renderer import render_report, render_report_from_store
from app.run.models import RunSpec
from app.run.orchestrator import ApplicationRunner

LOGGER = logging.getLogger(__name__)


class ApplicationSiteBuilder:
    """Application-level service that executes a run and renders the review site."""

    def __init__(
        self,
        run_spec: RunSpec,
        *,
        logger: logging.Logger = LOGGER,
    ) -> None:
        self.run_spec = run_spec
        self.logger = logger

    def build(self) -> Path:
        """Execute one configured run and render its static site output."""
        executed = ApplicationRunner(
            self.run_spec,
            logger=self.logger,
        ).execute()
        presentation_started = perf_counter()
        self.logger.info("[Presentation] Rendering static report artifacts...")
        if self.run_spec.parity_store_path is None:
            render_report(executed.run_result, executed.artifacts.site_dir)
        else:
            render_report_from_store(
                store_path=self.run_spec.parity_store_path,
                run_id=executed.run_result.run_id,
                output_dir=executed.artifacts.site_dir,
            )
        self.logger.info(
            "[Presentation] Report artifacts written to %s in %.1fs.",
            display_path(executed.artifacts.site_dir, self.run_spec.project_root),
            perf_counter() - presentation_started,
        )
        return executed.artifacts.site_dir


def build_site(run_spec: RunSpec) -> Path:
    """Build artifacts and render the static site for one explicit run spec."""
    return ApplicationSiteBuilder(run_spec).build()
