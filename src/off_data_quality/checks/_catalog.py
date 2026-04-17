from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from importlib import import_module
from types import MappingProxyType, ModuleType
from typing import TYPE_CHECKING

from off_data_quality.checks._legacy import legacy_code_template_key
from off_data_quality.checks._registry import (
    CheckBinding,
    CheckEvaluator,
    check_bindings,
)
from off_data_quality.checks._sources import (
    default_dsl_check_pack_resources,
    default_python_check_pack_module_names,
)
from off_data_quality.checks.dsl.evaluator import compile_dsl_evaluators
from off_data_quality.checks.dsl.parser import load_dsl_definitions
from off_data_quality.context._paths import path_spec_for
from off_data_quality.contracts.checks import (
    CheckDefinition,
    CheckSelection,
)

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping
    from importlib.resources.abc import Traversable


@dataclass(frozen=True, slots=True)
class CheckCatalog:
    """Immutable quality check catalog built from the definition sources."""

    checks: tuple[CheckDefinition, ...]
    evaluators_by_id: Mapping[str, CheckEvaluator]
    checks_by_id: Mapping[str, CheckDefinition]

    def check_by_id(self, check_id: str) -> CheckDefinition:
        """Return one check definition or fail explicitly when it is unknown."""
        try:
            return self.checks_by_id[check_id]
        except KeyError as exc:
            raise ValueError(f"Unknown check id: {check_id}") from exc

    def select_checks(
        self,
        active_check_ids: Collection[str] | None = None,
        *,
        selection: CheckSelection | None = None,
    ) -> tuple[CheckDefinition, ...]:
        """Return checks in catalog order that match the optional selection filters."""
        resolved_selection = selection or CheckSelection()
        active_id_set = _normalize_active_check_ids(active_check_ids)
        if active_id_set is not None:
            _validate_known_active_check_ids(self.checks_by_id, active_id_set)
            _validate_active_check_ids_match_selection(
                self.checks_by_id,
                active_id_set,
                resolved_selection,
            )
        return tuple(
            check
            for check in self.checks
            if _check_selected(check, active_id_set, resolved_selection)
        )

    def select_evaluators(
        self,
        active_check_ids: Collection[str] | None = None,
        *,
        selection: CheckSelection | None = None,
    ) -> dict[str, CheckEvaluator]:
        """Return evaluator bindings for the selected checks in catalog order."""
        return {
            check.id: self.evaluators_by_id[check.id]
            for check in self.select_checks(active_check_ids, selection=selection)
        }


def load_check_catalog(
    definitions_path: Traversable | None = None,
    *,
    definitions_paths: Collection[Traversable] | None = None,
    python_module_names: Collection[str] | None = None,
) -> CheckCatalog:
    """Load the check catalog and evaluator map from the definition sources."""
    resolved_definitions_paths = _resolve_definition_paths(
        definitions_path=definitions_path,
        definitions_paths=definitions_paths,
    )
    resolved_python_modules = _resolve_python_module_names(python_module_names)
    python_bindings = _load_python_check_bindings(resolved_python_modules)

    definitions: list[CheckDefinition] = []
    evaluators: dict[str, CheckEvaluator] = {}

    for binding in python_bindings:
        _validate_required_context_paths(binding.required_context_paths, binding.id)
        _register_definition(
            definitions=definitions,
            evaluators=evaluators,
            check_definition=CheckDefinition(
                id=binding.id,
                definition_language="python",
                required_context_paths=binding.required_context_paths,
                parity_baseline=binding.parity_baseline,
                jurisdictions=binding.jurisdictions,
            ),
            evaluator=binding.evaluator,
        )

    for definitions_resource in resolved_definitions_paths:
        dsl_definitions = load_dsl_definitions(definitions_resource)
        dsl_evaluators = compile_dsl_evaluators(dsl_definitions)
        for check in dsl_definitions:
            _validate_required_context_paths(check.required_context_paths, check.id)
            if check.id not in dsl_evaluators:
                raise ValueError(f"Missing DSL evaluator for {check.id}")
            _register_definition(
                definitions=definitions,
                evaluators=evaluators,
                check_definition=CheckDefinition(
                    id=check.id,
                    definition_language="dsl",
                    required_context_paths=check.required_context_paths,
                    parity_baseline=check.parity_baseline,
                    jurisdictions=check.jurisdictions,
                ),
                evaluator=dsl_evaluators[check.id],
            )

    ordered_checks = tuple(sorted(definitions, key=lambda check: check.id))
    _validate_unique_legacy_identities(ordered_checks)
    checks_by_id = {check.id: check for check in ordered_checks}
    ordered_evaluators = {check.id: evaluators[check.id] for check in ordered_checks}
    return CheckCatalog(
        checks=ordered_checks,
        evaluators_by_id=MappingProxyType(ordered_evaluators),
        checks_by_id=MappingProxyType(checks_by_id),
    )


@cache
def get_default_check_catalog() -> CheckCatalog:
    """Return the default quality check catalog for this process."""
    return load_check_catalog()


def _resolve_definition_paths(
    *,
    definitions_path: Traversable | None,
    definitions_paths: Collection[Traversable] | None,
) -> tuple[Traversable, ...]:
    """Return the DSL definition files that should be loaded for this catalog."""
    if definitions_path is not None and definitions_paths is not None:
        raise ValueError("Pass either definitions_path or definitions_paths, not both.")
    if definitions_paths is not None:
        return tuple(definitions_paths)
    if definitions_path is not None:
        return (definitions_path,)
    return default_dsl_check_pack_resources()


def _resolve_python_module_names(
    python_module_names: Collection[str] | None,
) -> tuple[str, ...]:
    """Return the Python check pack module names that should be loaded."""
    if python_module_names is None:
        return default_python_check_pack_module_names()
    return tuple(python_module_names)


def _load_python_check_bindings(
    python_module_names: Collection[str],
) -> tuple[CheckBinding, ...]:
    """Load defined by decorators Python checks from the configured pack modules."""
    seen_ids: set[str] = set()
    bindings: list[CheckBinding] = []
    for module_name in python_module_names:
        module = import_module(module_name)
        for binding in _collect_unique_python_bindings(module):
            if binding.id in seen_ids:
                raise ValueError(f"Duplicate check id: {binding.id}")
            seen_ids.add(binding.id)
            bindings.append(binding)
    return tuple(bindings)


def _collect_unique_python_bindings(module: ModuleType) -> tuple[CheckBinding, ...]:
    """Collect Python check bindings while rejecting duplicate canonical ids."""
    bindings = check_bindings(module)
    seen_ids: set[str] = set()
    for binding in bindings:
        if binding.id in seen_ids:
            raise ValueError(f"Duplicate check id: {binding.id}")
        seen_ids.add(binding.id)
    return bindings


def _register_definition(
    *,
    definitions: list[CheckDefinition],
    evaluators: dict[str, CheckEvaluator],
    check_definition: CheckDefinition,
    evaluator: CheckEvaluator,
) -> None:
    """Register one check definition and its evaluator while enforcing unique ids."""
    if check_definition.id in evaluators:
        raise ValueError(f"Duplicate check id: {check_definition.id}")
    definitions.append(check_definition)
    evaluators[check_definition.id] = evaluator


def _validate_unique_legacy_identities(checks: tuple[CheckDefinition, ...]) -> None:
    """Reject multiple checks that map to the same canonical legacy identity."""
    check_ids_by_legacy_key: dict[str, list[str]] = {}
    for check in checks:
        legacy_identity = check.legacy_identity
        if legacy_identity is None:
            continue
        check_ids_by_legacy_key.setdefault(
            legacy_code_template_key(legacy_identity.code_template),
            [],
        ).append(check.id)

    duplicate_legacy_mappings = {
        legacy_key: tuple(sorted(check_ids))
        for legacy_key, check_ids in check_ids_by_legacy_key.items()
        if len(check_ids) > 1
    }
    if not duplicate_legacy_mappings:
        return

    details = "; ".join(
        f"{legacy_key}: {', '.join(check_ids)}"
        for legacy_key, check_ids in sorted(duplicate_legacy_mappings.items())
    )
    raise ValueError(f"Duplicate legacy identity mappings: {details}")


def _validate_required_context_paths(
    required_context_paths: tuple[str, ...],
    check_id: str,
) -> None:
    """Ensure declared check dependencies refer to known check context paths."""
    unknown_paths = [
        path for path in required_context_paths if path_spec_for(path) is None
    ]
    if unknown_paths:
        raise ValueError(
            f"Check {check_id} declares unknown context paths: {', '.join(unknown_paths)}"
        )


def _normalize_active_check_ids(
    active_check_ids: Collection[str] | None,
) -> set[str] | None:
    """Return active check ids as a set for fast selection lookups."""
    if active_check_ids is None:
        return None
    return set(active_check_ids)


def _validate_known_active_check_ids(
    checks_by_id: Mapping[str, CheckDefinition],
    active_check_ids: set[str],
) -> None:
    """Fail explicitly when one requested check id is not in the catalog."""
    unknown_ids = sorted(active_check_ids - set(checks_by_id))
    if unknown_ids:
        raise ValueError(f"Unknown active check ids: {', '.join(unknown_ids)}")


def _validate_active_check_ids_match_selection(
    checks_by_id: Mapping[str, CheckDefinition],
    active_check_ids: set[str],
    selection: CheckSelection,
) -> None:
    """Fail when explicitly requested check ids do not satisfy the selection."""
    if selection == CheckSelection():
        return
    unavailable_ids = sorted(
        check_id
        for check_id in active_check_ids
        if not checks_by_id[check_id].matches_selection(selection)
    )
    if unavailable_ids:
        raise ValueError(
            "Checks not available for "
            f"{_selection_label(selection)}: {', '.join(unavailable_ids)}"
        )


def _selection_label(selection: CheckSelection) -> str:
    """Return a readable label describing one selection filter set."""
    parts: list[str] = []
    if selection.parity_baselines is not None:
        parts.append(
            "parity baselines " + ", ".join(sorted(selection.parity_baselines))
        )
    if selection.jurisdictions is not None:
        parts.append("jurisdictions " + ", ".join(selection.jurisdictions))
    return "; ".join(parts) if parts else "the active selection"


def _check_selected(
    check: CheckDefinition,
    active_check_ids: set[str] | None,
    selection: CheckSelection,
) -> bool:
    """Return whether one check survives the active id and metadata filters."""
    if active_check_ids is not None and check.id not in active_check_ids:
        return False
    return check.matches_selection(selection)
