from __future__ import annotations

import csv
import json
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol

import pytest
from app.reference.models import LegacyCheckTags, ReferenceResult

from openfoodfacts_data_quality.checks.catalog import (
    CheckCatalog,
    get_default_check_catalog,
)
from openfoodfacts_data_quality.contracts.checks import (
    CheckDefinition,
    CheckEmission,
    CheckJurisdiction,
    CheckParityBaseline,
    Severity,
)
from openfoodfacts_data_quality.contracts.context import (
    CategoryPropsContext,
    CheckContext,
    FlagsContext,
    NutritionContext,
    ProductContext,
)
from openfoodfacts_data_quality.contracts.enrichment import EnrichedSnapshot
from openfoodfacts_data_quality.contracts.findings import Finding
from openfoodfacts_data_quality.contracts.observations import (
    ObservationSide,
    ObservedFinding,
)
from openfoodfacts_data_quality.contracts.run import RunCheckResult, RunResult

Payload = dict[str, Any]
TEST_DEFAULT_PARITY_BASELINE: CheckParityBaseline = "legacy"
TEST_DEFAULT_JURISDICTIONS: tuple[CheckJurisdiction, ...] = ("global",)


class ContextFactory(Protocol):
    def __call__(
        self,
        *,
        code: str = "0000000000000",
        product: Payload | None = None,
        flags: Payload | None = None,
        category_props: Payload | None = None,
        nutrition: Payload | None = None,
    ) -> CheckContext: ...


class ReferenceResultFactory(Protocol):
    def __call__(
        self,
        *,
        code: str = "0000000000000",
        enriched_snapshot: Payload | None = None,
        legacy_check_tags: Payload | None = None,
    ) -> ReferenceResult: ...


class FindingFactory(Protocol):
    def __call__(
        self,
        *,
        check_id: str,
        product_id: str = "0000000000000",
        severity: Severity,
        emitted_code: str | None = None,
    ) -> Finding: ...


class ObservedFindingFactory(Protocol):
    def __call__(
        self,
        *,
        check_id: str,
        product_id: str = "0000000000000",
        severity: Severity,
        observed_code: str | None = None,
        side: ObservationSide = "migrated",
    ) -> ObservedFinding: ...


class RunResultFactory(Protocol):
    def __call__(self) -> RunResult: ...


class CheckDefinitionFactory(Protocol):
    def __call__(
        self,
        check_id: str,
        *,
        parity_baseline: CheckParityBaseline = TEST_DEFAULT_PARITY_BASELINE,
        jurisdictions: tuple[CheckJurisdiction, ...] = TEST_DEFAULT_JURISDICTIONS,
    ) -> CheckDefinition: ...


class CatalogWithChecksFactory(Protocol):
    def __call__(self, *checks: CheckDefinition) -> CheckCatalog: ...


class LegacySourceRootFactory(Protocol):
    def __call__(self, tmp_path: Path) -> Path: ...


class MigrationInventoryFactory(Protocol):
    def __call__(self, tmp_path: Path) -> tuple[Path, Path]: ...


@pytest.fixture
def default_check_catalog() -> CheckCatalog:
    return get_default_check_catalog()


@pytest.fixture
def default_checks_by_id(
    default_check_catalog: CheckCatalog,
) -> Mapping[str, CheckDefinition]:
    return default_check_catalog.checks_by_id


@pytest.fixture
def context_factory() -> ContextFactory:
    def factory(
        *,
        code: str = "0000000000000",
        product: Payload | None = None,
        flags: Payload | None = None,
        category_props: Payload | None = None,
        nutrition: Payload | None = None,
    ) -> CheckContext:
        base_product: Payload = {
            "code": code,
            "lc": "en",
            "lang": "en",
            "created_t": 1.0,
            "product_name": "Example product",
            "quantity": "500 g",
            "packagings": [
                {"number": 1, "shape": "en:bottle", "material": "en:plastic"}
            ],
            "product_quantity": 500.0,
            "serving_size": "50 g",
            "serving_quantity": 50.0,
            "brands": "Example brand",
            "categories": "Example category",
            "labels": None,
            "emb_codes": None,
            "ingredients_text": "Sugar, salt",
            "ingredients": [],
            "ingredients_tags": [],
            "ingredients_percent_analysis": 0.0,
            "ingredients_with_specified_percent_n": 0.0,
            "ingredients_with_unspecified_percent_n": 0.0,
            "ingredients_with_specified_percent_sum": 0.0,
            "ingredients_with_unspecified_percent_sum": 0.0,
            "nutriscore_grade": None,
            "nutriscore_grade_producer": None,
            "nutriscore_score": None,
            "categories_tags": [],
            "labels_tags": [],
            "countries_tags": [],
            "food_groups_tags": [],
        }
        if product:
            base_product.update(product)

        base_flags: Payload = {
            "is_european_product": False,
            "has_animal_origin_category": False,
            "ignore_energy_calculated_error": False,
        }
        if flags:
            base_flags.update(flags)

        base_category_props: Payload = {
            "minimum_number_of_ingredients": None,
        }
        if category_props:
            base_category_props.update(category_props)

        base_nutrition: Payload = {
            "input_sets": [],
            "as_sold": {},
        }
        if nutrition:
            base_nutrition.update(nutrition)

        return CheckContext(
            code=code,
            product=ProductContext.model_validate(base_product),
            flags=FlagsContext.model_validate(base_flags),
            category_props=CategoryPropsContext.model_validate(base_category_props),
            nutrition=NutritionContext.model_validate(base_nutrition),
        )

    return factory


@pytest.fixture
def check_definition_factory() -> CheckDefinitionFactory:
    def factory(
        check_id: str,
        *,
        parity_baseline: CheckParityBaseline = TEST_DEFAULT_PARITY_BASELINE,
        jurisdictions: tuple[CheckJurisdiction, ...] = TEST_DEFAULT_JURISDICTIONS,
    ) -> CheckDefinition:
        return CheckDefinition(
            id=check_id,
            definition_language="dsl",
            parity_baseline=parity_baseline,
            jurisdictions=jurisdictions,
            required_context_paths=("product.code",),
        )

    return factory


@pytest.fixture
def catalog_with_checks_factory() -> CatalogWithChecksFactory:
    def noop_evaluator(_: object) -> list[CheckEmission]:
        return []

    def factory(*checks: CheckDefinition) -> CheckCatalog:
        evaluators = {check.id: noop_evaluator for check in checks}
        return CheckCatalog(
            checks=checks,
            evaluators_by_id=MappingProxyType(evaluators),
            checks_by_id=MappingProxyType({check.id: check for check in checks}),
        )

    return factory


@pytest.fixture
def legacy_source_root_factory() -> LegacySourceRootFactory:
    def factory(tmp_path: Path) -> Path:
        legacy_root = tmp_path / "legacy"
        product_opener_dir = legacy_root / "lib" / "ProductOpener"
        product_opener_dir.mkdir(parents=True)

        (product_opener_dir / "DataQualityCommon.pm").write_text(
            """
sub check_quantity_not_recognized ($product_ref) {
    push @{$product_ref->{data_quality_warnings_tags}}, "en:quantity-not-recognized";
    return;
}
""".strip(),
            encoding="utf-8",
        )
        (product_opener_dir / "DataQualityDimensions.pm").write_text(
            """
sub check_product_name_completion ($product_ref) {
    add_tag($product_ref, "data_quality_completeness", "en:product-name-to-be-completed");
    return;
}
""".strip(),
            encoding="utf-8",
        )
        (product_opener_dir / "DataQualityFood.pm").write_text(
            """
sub check_food_groups ($product_ref) {
    for (my $level = 1; $level <= 3; $level++) {
        if (deep_exists($product_ref, "food_groups_tags", $level - 1)) {
            push @{$product_ref->{data_quality_info_tags}}, 'en:food-groups-' . $level . '-known';
        }
        else {
            push @{$product_ref->{data_quality_info_tags}}, 'en:food-groups-' . $level . '-unknown';
        }
    }

    return;
}

sub check_energy_mismatch ($product_ref, $data_quality_tags, $set_id, $unit) {
    push @{$product_ref->{$data_quality_tags}},
        "en:${set_id}-energy-value-in-$unit-does-not-match-value-computed-from-other-nutrients";
    return;
}

sub check_sugars_starch_fiber ($product_ref, $data_quality_tags, $set_id) {
    push @{$product_ref->{$data_quality_tags}},
        "en:${set_id}-sugars-plus-starch-plus-fiber-greater-than-carbohydrates-total";
    return;
}

sub check_ingredients_language_mismatch ($product_ref, $lc) {
    add_tag(
        $product_ref,
        "data_quality_warnings",
        "en:ingredients-language-mismatch-" . $product_ref->{ingredients_lc} . "-contains-" . $lc
    );
    return;
}

sub check_vitamin_mineral_claim ($product_ref, $vit_or_min, $vit_or_min_label) {
    add_tag(
        $product_ref,
        "data_quality_warnings",
        "en:"
            . substr($vit_or_min_label, 3)
            . "-label-claim-but-$vit_or_min-below-$vitamins_and_minerals_labelling{europe}{$vit_or_min}{$vit_or_min_label}"
    );
    return;
}

sub check_specific_ingredient_quantity ($product_ref, $specific_ingredient_id, $quantity_threshold, $category_id) {
    add_tag(
        $product_ref,
        "data_quality_errors",
        "en:specific-ingredient-"
            . substr($specific_ingredient_id, 3)
            . "-quantity-is-below-the-minimum-value-of-$quantity_threshold-for-category-"
            . substr($category_id, 3)
    );
    return;
}

sub check_incompatible_tags ($product_ref) {
    add_tag(
        $product_ref,
        "data_quality_errors",
        "en:mutually-exclusive-tags-for-$incompatible_tags[0]-and-$incompatible_tags[1]"
    );
    return;
}
""".strip(),
            encoding="utf-8",
        )
        return legacy_root

    return factory


@pytest.fixture
def migration_inventory_factory() -> MigrationInventoryFactory:
    def factory(tmp_path: Path) -> tuple[Path, Path]:
        artifact_path = tmp_path / "legacy_families.json"
        estimation_path = tmp_path / "estimation_sheet.csv"
        artifact_path.write_text(
            json.dumps(
                {
                    "version": 2,
                    "families": [
                        {
                            "check_id": "en:legacy-check-a",
                            "template_key": "en:legacy-check-a",
                            "code_templates": ["en:legacy-check-a"],
                            "placeholder_names": [],
                            "placeholder_count": 0,
                            "features": {
                                "has_loop": False,
                                "has_branching": False,
                                "has_arithmetic": False,
                                "helper_calls": [],
                                "source_files_count": 1,
                                "source_subroutines_count": 1,
                                "unsupported_data_quality_emission_count_total": 0,
                                "line_span_max": 10,
                                "statement_count_max": 3,
                            },
                        },
                        {
                            "check_id": "en:legacy-check-b",
                            "template_key": "en:legacy-check-b",
                            "code_templates": ["en:legacy-check-b"],
                            "placeholder_names": [],
                            "placeholder_count": 0,
                            "features": {
                                "has_loop": False,
                                "has_branching": False,
                                "has_arithmetic": False,
                                "helper_calls": [],
                                "source_files_count": 1,
                                "source_subroutines_count": 1,
                                "unsupported_data_quality_emission_count_total": 0,
                                "line_span_max": 11,
                                "statement_count_max": 4,
                            },
                        },
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        with estimation_path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=[
                    "check_id",
                    "target_impl",
                    "size",
                    "risk",
                    "estimated_hours",
                    "rationale",
                ],
            )
            writer.writeheader()
            writer.writerows(
                [
                    {
                        "check_id": "en:legacy-check-a",
                        "target_impl": "dsl",
                        "size": "S",
                        "risk": "low",
                        "estimated_hours": "1",
                        "rationale": "Simple DSL migration.",
                    },
                    {
                        "check_id": "en:legacy-check-b",
                        "target_impl": "python",
                        "size": "L",
                        "risk": "high",
                        "estimated_hours": "8",
                        "rationale": "Complex helper-heavy migration.",
                    },
                ]
            )
        return artifact_path, estimation_path

    return factory


@pytest.fixture
def reference_result_factory() -> ReferenceResultFactory:
    def factory(
        *,
        code: str = "0000000000000",
        enriched_snapshot: Payload | None = None,
        legacy_check_tags: Payload | None = None,
    ) -> ReferenceResult:
        return ReferenceResult(
            code=code,
            enriched_snapshot=EnrichedSnapshot.model_validate(
                enriched_snapshot
                or {
                    "product": {"code": code},
                    "flags": {},
                    "category_props": {},
                    "nutrition": {},
                }
            ),
            legacy_check_tags=LegacyCheckTags.model_validate(legacy_check_tags or {}),
        )

    return factory


@pytest.fixture
def finding_factory() -> FindingFactory:
    def factory(
        *,
        check_id: str,
        product_id: str = "0000000000000",
        severity: Severity,
        emitted_code: str | None = None,
    ) -> Finding:
        return Finding(
            product_id=product_id,
            check_id=check_id,
            severity=severity,
            emitted_code=emitted_code,
        )

    return factory


@pytest.fixture
def observed_finding_factory() -> ObservedFindingFactory:
    def factory(
        *,
        check_id: str,
        product_id: str = "0000000000000",
        severity: Severity,
        observed_code: str | None = None,
        side: ObservationSide = "migrated",
    ) -> ObservedFinding:
        return ObservedFinding(
            product_id=product_id,
            check_id=check_id,
            observed_code=observed_code or check_id,
            severity=severity,
            side=side,
        )

    return factory


@pytest.fixture
def run_result_factory(
    observed_finding_factory: ObservedFindingFactory,
    default_checks_by_id: Mapping[str, CheckDefinition],
) -> RunResultFactory:
    def factory() -> RunResult:
        check_pass = RunCheckResult(
            definition=default_checks_by_id["en:product-name-to-be-completed"],
            reference_count=1,
            migrated_count=1,
            matched_count=1,
            missing_count=0,
            extra_count=0,
            missing=[],
            extra=[],
            passed=True,
        )
        check_fail = RunCheckResult(
            definition=default_checks_by_id["en:quantity-not-recognized"],
            reference_count=1,
            migrated_count=0,
            matched_count=0,
            missing_count=1,
            extra_count=0,
            missing=[
                observed_finding_factory(
                    check_id="en:quantity-not-recognized",
                    product_id="123",
                    severity="warning",
                    side="reference",
                )
            ],
            extra=[],
            passed=False,
        )
        return RunResult(
            run_id="test-run",
            source_snapshot_id="source-snapshot",
            product_count=2,
            checks=[check_pass, check_fail],
            compared_check_count=2,
            runtime_only_check_count=0,
            reference_total=2,
            compared_migrated_total=1,
            matched_total=1,
            runtime_only_migrated_total=0,
        )

    return factory
