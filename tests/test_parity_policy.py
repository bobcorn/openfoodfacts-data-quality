from __future__ import annotations

from pathlib import Path

import pytest
from app.parity.policy import (
    ExpectedDifferencesRegistry,
    load_expected_differences_registry,
)

from openfoodfacts_data_quality.contracts.observations import ObservedFinding


def test_load_expected_differences_registry_returns_empty_when_disabled() -> None:
    registry = load_expected_differences_registry(None)

    assert registry == ExpectedDifferencesRegistry()
    assert registry.rule_count == 0


def test_load_expected_differences_registry_matches_specific_missing_rule(
    tmp_path: Path,
) -> None:
    registry_path = tmp_path / "expected-differences.toml"
    registry_path.write_text(
        """
schema_version = 1

[[rules]]
id = "quantity-known-gap"
justification = "Known migration gap under review."
check_id = "en:quantity-not-recognized"
mismatch_kind = "missing"
severity = "warning"
""".strip(),
        encoding="utf-8",
    )

    registry = load_expected_differences_registry(registry_path)
    matched_rule = registry.classify(
        kind="missing",
        finding=ObservedFinding(
            check_id="en:quantity-not-recognized",
            product_id="123",
            observed_code="en:quantity-not-recognized",
            severity="warning",
            side="reference",
        ),
    )

    assert matched_rule is not None
    assert matched_rule.id == "quantity-known-gap"
    assert matched_rule.justification == "Known migration gap under review."
    assert registry.source_path == registry_path.resolve()


def test_expected_differences_registry_rejects_overlapping_rules(
    tmp_path: Path,
) -> None:
    registry_path = tmp_path / "expected-differences.toml"
    registry_path.write_text(
        """
schema_version = 1

[[rules]]
id = "rule-a"
justification = "First overlap."
check_id = "en:quantity-not-recognized"
mismatch_kind = "missing"

[[rules]]
id = "rule-b"
justification = "Second overlap."
check_id = "en:quantity-not-recognized"
mismatch_kind = "missing"
""".strip(),
        encoding="utf-8",
    )

    registry = load_expected_differences_registry(registry_path)

    with pytest.raises(ValueError, match="overlapping rules"):
        registry.classify(
            kind="missing",
            finding=ObservedFinding(
                check_id="en:quantity-not-recognized",
                product_id="123",
                observed_code="en:quantity-not-recognized",
                severity="warning",
                side="reference",
            ),
        )


def test_load_expected_differences_registry_rejects_duplicate_rule_ids(
    tmp_path: Path,
) -> None:
    registry_path = tmp_path / "expected-differences.toml"
    registry_path.write_text(
        """
schema_version = 1

[[rules]]
id = "quantity-known-gap"
justification = "Known migration gap under review."
check_id = "en:quantity-not-recognized"
mismatch_kind = "missing"

[[rules]]
id = "quantity-known-gap"
justification = "Same id, different rule."
check_id = "en:product-name-to-be-completed"
mismatch_kind = "extra"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate rule ids"):
        load_expected_differences_registry(registry_path)
