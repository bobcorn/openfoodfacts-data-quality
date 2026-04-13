from __future__ import annotations

import logging

from pytest import MonkeyPatch

from runtime_support.logging_config import (
    CLI_LOG_FORMAT,
    CLI_LOG_SOURCE_FORMAT,
    LOG_INCLUDE_SOURCE_ENV_VAR,
    configure_cli_logging,
    configured_cli_log_format,
    configured_log_include_source,
)


def test_configured_log_include_source_defaults_to_false(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.delenv(LOG_INCLUDE_SOURCE_ENV_VAR, raising=False)

    assert configured_log_include_source() is False
    assert configured_cli_log_format() == CLI_LOG_FORMAT


def test_configured_log_include_source_accepts_truthy_values(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv(LOG_INCLUDE_SOURCE_ENV_VAR, "true")

    assert configured_log_include_source() is True
    assert configured_cli_log_format() == CLI_LOG_SOURCE_FORMAT


def test_configure_cli_logging_uses_selected_format(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv(LOG_INCLUDE_SOURCE_ENV_VAR, "1")

    configure_cli_logging()

    root_handler = logging.getLogger().handlers[0]
    assert root_handler.formatter is not None
    assert root_handler.formatter._fmt == CLI_LOG_SOURCE_FORMAT
