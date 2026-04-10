"""Supported catalog surface for packaged data quality checks."""

from __future__ import annotations

from off_data_quality.checks._catalog import (
    CheckCatalog,
    get_default_check_catalog,
    load_check_catalog,
)
from off_data_quality.checks._legacy import (
    LegacyCheckIndex,
    has_legacy_code_template_tokens,
    legacy_code_template_key,
    legacy_code_template_placeholders,
    matches_legacy_check_code,
)

__all__ = [
    "CheckCatalog",
    "LegacyCheckIndex",
    "get_default_check_catalog",
    "has_legacy_code_template_tokens",
    "legacy_code_template_key",
    "legacy_code_template_placeholders",
    "load_check_catalog",
    "matches_legacy_check_code",
]
