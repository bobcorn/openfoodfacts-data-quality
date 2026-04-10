"""Supported context provider surface for the data quality runtime."""

from __future__ import annotations

from off_data_quality.context._builder import (
    build_enriched_snapshot_context,
    build_enriched_snapshot_contexts,
    build_enriched_snapshot_record_context,
    build_source_product_contexts,
    iter_enriched_snapshot_contexts,
    iter_source_product_contexts,
)
from off_data_quality.context._paths import (
    MISSING,
    PATH_SPECS,
    ContextPathSpec,
    is_blank,
    path_spec_for,
    resolve_path,
)
from off_data_quality.context._providers import (
    CHECK_CONTEXT_PROVIDERS,
    ENRICHED_SNAPSHOTS_PROVIDER,
    SOURCE_PRODUCTS_PROVIDER,
    ContextProvider,
    ContextProviderId,
    context_availability_for_provider,
    validate_context_provider,
)

__all__ = [
    "CHECK_CONTEXT_PROVIDERS",
    "ENRICHED_SNAPSHOTS_PROVIDER",
    "MISSING",
    "PATH_SPECS",
    "SOURCE_PRODUCTS_PROVIDER",
    "ContextPathSpec",
    "ContextProvider",
    "ContextProviderId",
    "build_enriched_snapshot_context",
    "build_enriched_snapshot_contexts",
    "build_enriched_snapshot_record_context",
    "build_source_product_contexts",
    "context_availability_for_provider",
    "is_blank",
    "iter_enriched_snapshot_contexts",
    "iter_source_product_contexts",
    "path_spec_for",
    "resolve_path",
    "validate_context_provider",
]
