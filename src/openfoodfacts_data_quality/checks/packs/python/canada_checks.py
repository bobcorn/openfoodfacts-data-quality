"""Packaged Canada-specific Python quality checks.

These prototype checks intentionally use canonical claim tags together with
per-100g scalar nutrients until the runtime models Canadian reference amounts
and serving-of-stated-size rules explicitly.

Source authority: Health Canada.
Source document: "Nutrition Labelling — Table of Permitted Nutrient Content
Statements and Claims" (published September 2, 2025).
Implemented reference items: 22.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from openfoodfacts_data_quality.checks.check_helpers import single_emission
from openfoodfacts_data_quality.checks.registry import check
from openfoodfacts_data_quality.contracts.checks import CheckEmission, CheckPackMetadata
from openfoodfacts_data_quality.scalars import as_number

if TYPE_CHECKING:
    from openfoodfacts_data_quality.contracts.context import NormalizedContext

CHECK_PACK_METADATA = CheckPackMetadata(
    parity_baseline="none",
    jurisdictions=("ca",),
)


@check(
    "ca:trans-fat-free-claim-but-nutrition-does-not-meet-conditions",
    requires=(
        "product.labels_tags",
        "nutrition.as_sold.energy_kcal",
        "nutrition.as_sold.saturated_fat",
        "nutrition.as_sold.trans_fat",
    ),
)
def ca_trans_fat_free_claim_but_nutrition_does_not_meet_conditions(
    context: NormalizedContext,
) -> list[CheckEmission]:
    """Flag products whose trans-fat-free claim conflicts with Canada rules."""
    if "en:trans-fat-free" not in set(context.product.labels_tags):
        return []

    energy_kcal = as_number(context.nutrition.as_sold.energy_kcal)
    saturated_fat = as_number(context.nutrition.as_sold.saturated_fat)
    trans_fat = as_number(context.nutrition.as_sold.trans_fat)
    if trans_fat is None or saturated_fat is None or energy_kcal is None:
        return []

    saturated_and_trans_fat = saturated_fat + trans_fat
    if trans_fat >= 0.2 or saturated_and_trans_fat > 2:
        return single_emission("warning")
    if energy_kcal <= 0:
        return []

    percent_energy_from_saturated_and_trans_fat = (
        saturated_and_trans_fat * 9 / energy_kcal * 100
    )
    if percent_energy_from_saturated_and_trans_fat > 15:
        return single_emission("warning")
    return []
