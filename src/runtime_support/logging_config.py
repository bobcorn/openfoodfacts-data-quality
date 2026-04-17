from __future__ import annotations

import logging
import os
from collections.abc import Mapping

LOG_INCLUDE_SOURCE_ENV_VAR = "LOG_INCLUDE_SOURCE"
CLI_LOG_FORMAT = "%(asctime)s %(levelname)-8s %(message)s"
CLI_LOG_SOURCE_FORMAT = "%(asctime)s %(levelname)-8s %(name)s:%(lineno)d %(message)s"
CLI_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_TRUE_ENV_VALUES = frozenset({"1", "true", "yes", "on"})


def configure_cli_logging(
    *,
    environ: Mapping[str, str] | None = None,
) -> None:
    """Configure the root logger for local CLI entrypoints."""
    logging.basicConfig(
        level=logging.INFO,
        format=configured_cli_log_format(environ=environ),
        datefmt=CLI_LOG_DATE_FORMAT,
        force=True,
    )


def configured_cli_log_format(
    *,
    environ: Mapping[str, str] | None = None,
) -> str:
    """Return the local CLI log format selected by environment."""
    if configured_log_include_source(environ=environ):
        return CLI_LOG_SOURCE_FORMAT
    return CLI_LOG_FORMAT


def configured_log_include_source(
    *,
    environ: Mapping[str, str] | None = None,
) -> bool:
    """Return whether local CLI logs should include logger name and line number."""
    resolved_environ = os.environ if environ is None else environ
    configured = resolved_environ.get(LOG_INCLUDE_SOURCE_ENV_VAR)
    if configured is None:
        return False
    return configured.strip().lower() in _TRUE_ENV_VALUES


__all__ = [
    "CLI_LOG_DATE_FORMAT",
    "CLI_LOG_FORMAT",
    "CLI_LOG_SOURCE_FORMAT",
    "LOG_INCLUDE_SOURCE_ENV_VAR",
    "configure_cli_logging",
    "configured_cli_log_format",
    "configured_log_include_source",
]
