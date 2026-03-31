from __future__ import annotations

import json
from importlib.resources import files
from typing import get_args

from openfoodfacts_data_quality.checks.dsl.ast import Operator
from openfoodfacts_data_quality.context.paths import PATH_SPECS


def test_dsl_capabilities_manifest_matches_supported_operators_and_context_roots() -> (
    None
):
    payload = json.loads(
        files("openfoodfacts_data_quality.checks.dsl")
        .joinpath("capabilities.json")
        .read_text(encoding="utf-8")
    )

    assert payload["kind"] == "openfoodfacts_dqm.dsl_capabilities"
    assert payload["version"] == 1
    assert payload["logical_forms"] == ["all", "any", "not"]
    assert payload["atom_operators"] == list(get_args(Operator))
    assert payload["field_constraints"]["supported_context_roots"] == sorted(
        {spec.path.split(".")[0] for spec in PATH_SPECS if spec.dsl_allowed}
    )
    assert payload["emission_constraints"]["max_findings_per_check_evaluation"] == 1
    assert payload["emission_constraints"]["dynamic_observed_code"] is False
