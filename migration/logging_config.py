from __future__ import annotations

import logging

CLI_LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s %(message)s"
CLI_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_cli_logging() -> None:
    """Configure the root logger for local CLI-oriented entrypoints."""
    logging.basicConfig(
        level=logging.INFO,
        format=CLI_LOG_FORMAT,
        datefmt=CLI_LOG_DATE_FORMAT,
        force=True,
    )
