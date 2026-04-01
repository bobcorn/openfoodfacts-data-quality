from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from openfoodfacts_data_quality.contracts.checks import (
    CheckEmission,
    CheckJurisdiction,
    CheckPackMetadata,
    CheckParityBaseline,
    LegacyCheckIdentity,
    resolve_legacy_check_identity,
)
from openfoodfacts_data_quality.contracts.context import NormalizedContext

if TYPE_CHECKING:
    from types import ModuleType

CheckEvaluator = Callable[[NormalizedContext], list[CheckEmission]]
_CHECK_BINDING_ATTR = "__openfoodfacts_data_quality_check_binding__"
_CHECK_PACK_METADATA_ATTR = "CHECK_PACK_METADATA"


@dataclass(frozen=True, slots=True)
class CheckBinding:
    """One registered check together with its runtime metadata."""

    id: str
    evaluator: CheckEvaluator
    required_context_paths: tuple[str, ...]
    parity_baseline: CheckParityBaseline
    jurisdictions: tuple[CheckJurisdiction, ...]
    legacy_identity: LegacyCheckIdentity | None = None

    def __post_init__(self) -> None:
        """Resolve the explicit legacy identity implied by the parity baseline."""
        object.__setattr__(
            self,
            "legacy_identity",
            resolve_legacy_check_identity(
                check_id=self.id,
                parity_baseline=self.parity_baseline,
                legacy_identity=self.legacy_identity,
            ),
        )


@dataclass(frozen=True, slots=True)
class _PendingCheckBinding:
    """Decorator-collected check metadata before pack metadata is applied."""

    id: str
    evaluator: CheckEvaluator
    required_context_paths: tuple[str, ...]
    legacy_identity: LegacyCheckIdentity | None = None


def check(
    check_id: str,
    *,
    requires: tuple[str, ...] = (),
    legacy_code_template: str | None = None,
) -> Callable[[CheckEvaluator], CheckEvaluator]:
    """Attach check metadata to one evaluator definition."""

    def decorator(evaluator: CheckEvaluator) -> CheckEvaluator:
        setattr(
            evaluator,
            _CHECK_BINDING_ATTR,
            _PendingCheckBinding(
                id=check_id,
                evaluator=evaluator,
                required_context_paths=requires,
                legacy_identity=(
                    LegacyCheckIdentity(code_template=legacy_code_template)
                    if legacy_code_template is not None
                    else None
                ),
            ),
        )
        return evaluator

    return decorator


def check_bindings(module: ModuleType) -> tuple[CheckBinding, ...]:
    """Collect defined by decorators checks with required pack metadata applied."""
    pack_metadata = _required_pack_metadata_for(module)
    bindings: list[CheckBinding] = []
    for value in module.__dict__.values():
        pending_binding = getattr(value, _CHECK_BINDING_ATTR, None)
        if not isinstance(pending_binding, _PendingCheckBinding):
            continue
        bindings.append(
            CheckBinding(
                id=pending_binding.id,
                evaluator=pending_binding.evaluator,
                required_context_paths=pending_binding.required_context_paths,
                parity_baseline=pack_metadata.parity_baseline,
                jurisdictions=pack_metadata.jurisdictions,
                legacy_identity=pending_binding.legacy_identity,
            )
        )
    return tuple(bindings)


def _required_pack_metadata_for(module: ModuleType) -> CheckPackMetadata:
    """Return the required module-level check pack metadata block."""
    metadata = getattr(module, _CHECK_PACK_METADATA_ATTR, None)
    if isinstance(metadata, CheckPackMetadata):
        return metadata
    if metadata is None:
        raise ValueError(
            f"Python check pack {module.__name__} must declare CHECK_PACK_METADATA."
        )
    raise ValueError(
        f"Python check pack {module.__name__} must bind CHECK_PACK_METADATA "
        "to a CheckPackMetadata instance."
    )
