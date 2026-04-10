from __future__ import annotations

import logging
from pathlib import Path
from time import perf_counter

from migration.artifacts import display_path
from migration.report.renderer import render_report, render_report_from_store
from migration.run.models import RunSpec
from migration.run.orchestrator import MigrationRunner

LOGGER = logging.getLogger(__name__)


class ReportSiteBuilder:
    """Service that executes one migration run and renders the review site."""

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
        executed = MigrationRunner(
            self.run_spec,
            logger=self.logger,
        ).execute()
        presentation_started = perf_counter()
        self.logger.info("[Presentation] Rendering static report artifacts...")
        if self.run_spec.parity_store_path is None:
            render_report(
                executed.run_result,
                executed.artifact_workspace.site_dir,
                source_input_summary=executed.source_input_summary,
            )
        else:
            render_report_from_store(
                store_path=self.run_spec.parity_store_path,
                run_id=executed.run_result.run_id,
                output_dir=executed.artifact_workspace.site_dir,
            )
        self.logger.info(
            "[Presentation] Report artifacts written to %s in %.1fs.",
            display_path(
                executed.artifact_workspace.site_dir,
                self.run_spec.project_root,
            ),
            perf_counter() - presentation_started,
        )
        return executed.artifact_workspace.site_dir


def build_site(run_spec: RunSpec) -> Path:
    """Build artifacts and render the static site for one explicit run spec."""
    return ReportSiteBuilder(run_spec).build()
