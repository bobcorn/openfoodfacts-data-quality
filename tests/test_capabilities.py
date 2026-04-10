from __future__ import annotations

from off_data_quality.contracts.capabilities import (
    CheckCapability,
    CheckCapabilityReport,
    ContextAvailability,
    missing_context_paths,
    resolve_check_capabilities,
    supports_context_paths,
)
from off_data_quality.contracts.checks import CheckDefinition


def _check_definition(
    check_id: str,
    required_context_paths: tuple[str, ...],
) -> CheckDefinition:
    return CheckDefinition(
        id=check_id,
        definition_language="python",
        parity_baseline="legacy",
        jurisdictions=("global",),
        required_context_paths=required_context_paths,
    )


def test_check_context_availability_records_available_paths() -> None:
    availability = ContextAvailability(
        available_context_paths=frozenset(
            (
                "product.code",
                "product.categories_tags",
            )
        ),
    )

    assert availability.available_context_paths == frozenset(
        (
            "product.code",
            "product.categories_tags",
        )
    )


def test_context_path_support_reports_missing_paths() -> None:
    availability = ContextAvailability(
        available_context_paths=frozenset(("product.code", "product.quantity")),
    )

    assert supports_context_paths(("product.code",), availability)
    assert not supports_context_paths(
        ("product.code", "product.categories_tags"),
        availability,
    )
    assert missing_context_paths(
        ("product.code", "product.categories_tags"),
        availability,
    ) == ("product.categories_tags",)


def test_check_capability_reports_runnable_and_unsupported_checks() -> None:
    report = resolve_check_capabilities(
        (
            _check_definition("en:code-check", ("product.code",)),
            _check_definition(
                "en:category-check",
                ("product.code", "product.categories_tags"),
            ),
        ),
        ContextAvailability(
            available_context_paths=frozenset(("product.code",)),
        ),
    )

    assert report == CheckCapabilityReport(
        runnable_capabilities=(
            CheckCapability(
                check_id="en:code-check",
                required_context_paths=("product.code",),
                missing_context_paths=(),
            ),
        ),
        unsupported_capabilities=(
            CheckCapability(
                check_id="en:category-check",
                required_context_paths=("product.code", "product.categories_tags"),
                missing_context_paths=("product.categories_tags",),
            ),
        ),
    )
    assert tuple(
        capability.check_id for capability in report.runnable_capabilities
    ) == ("en:code-check",)
    assert tuple(
        capability.check_id for capability in report.unsupported_capabilities
    ) == ("en:category-check",)
