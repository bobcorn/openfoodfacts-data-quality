from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

PlanningRisk = Literal["low", "medium", "high"]
PlanningSize = Literal["S", "M", "L"]
PlanningTargetImpl = Literal["dsl", "python"]

_TARGET_IMPL_VALUES = frozenset({"dsl", "python"})
_SIZE_VALUES = frozenset({"S", "M", "L"})
_RISK_VALUES = frozenset({"low", "medium", "high"})
_REQUIRED_ESTIMATION_COLUMNS = frozenset(
    {
        "check_id",
        "target_impl",
        "size",
        "risk",
        "estimated_hours",
        "rationale",
    }
)


@dataclass(frozen=True, slots=True)
class MigrationAssessment:
    """Planning metadata optionally attached to one legacy family."""

    target_impl: PlanningTargetImpl | None = None
    size: PlanningSize | None = None
    risk: PlanningRisk | None = None
    estimated_hours: str | None = None
    rationale: str | None = None

    @property
    def is_complete(self) -> bool:
        """Return whether the planning fields are fully populated."""
        return all(
            value is not None
            for value in (
                self.target_impl,
                self.size,
                self.risk,
                self.rationale,
            )
        )


@dataclass(frozen=True, slots=True)
class MigrationFamily:
    """One legacy emission family joined with optional planning metadata."""

    check_id: str
    template_key: str
    code_templates: tuple[str, ...]
    placeholder_names: tuple[str, ...]
    placeholder_count: int
    has_loop: bool
    has_branching: bool
    has_arithmetic: bool
    helper_calls: tuple[str, ...]
    source_files_count: int
    source_subroutines_count: int
    unsupported_data_quality_emission_count_total: int
    line_span_max: int
    statement_count_max: int
    assessment: MigrationAssessment | None = None

    @property
    def target_impl(self) -> PlanningTargetImpl | None:
        """Return the planned implementation target, if assessed."""
        return None if self.assessment is None else self.assessment.target_impl

    @property
    def size(self) -> PlanningSize | None:
        """Return the assessed size bucket, if any."""
        return None if self.assessment is None else self.assessment.size

    @property
    def risk(self) -> PlanningRisk | None:
        """Return the assessed migration risk, if any."""
        return None if self.assessment is None else self.assessment.risk

    @property
    def estimated_hours(self) -> str | None:
        """Return the estimated migration effort, if recorded."""
        return None if self.assessment is None else self.assessment.estimated_hours

    @property
    def rationale(self) -> str | None:
        """Return the planning rationale, if recorded."""
        return None if self.assessment is None else self.assessment.rationale

    @property
    def is_assessed(self) -> bool:
        """Return whether the family has a complete planning assessment."""
        return self.assessment is not None and self.assessment.is_complete


@dataclass(frozen=True, slots=True)
class ActiveMigrationPlan:
    """Migration-family coverage for one active run workset."""

    families: tuple[MigrationFamily, ...] = ()
    missing_check_ids: tuple[str, ...] = ()

    @property
    def family_count(self) -> int:
        """Return the number of active checks matched to migration families."""
        return len(self.families)

    @property
    def assessed_family_count(self) -> int:
        """Return the number of active matched families with planning metadata."""
        return sum(1 for family in self.families if family.is_assessed)


@dataclass(frozen=True, slots=True)
class MigrationCatalog:
    """Legacy family inventory joined with optional estimation-sheet metadata."""

    families: tuple[MigrationFamily, ...] = ()
    artifact_path: Path | None = None
    estimation_sheet_path: Path | None = None

    @property
    def family_count(self) -> int:
        """Return the number of known migration families."""
        return len(self.families)

    @property
    def assessed_family_count(self) -> int:
        """Return the number of families with a complete assessment."""
        return sum(1 for family in self.families if family.is_assessed)

    @property
    def families_by_check_id(self) -> dict[str, MigrationFamily]:
        """Return the families indexed by canonical check id."""
        return {family.check_id: family for family in self.families}

    def active_plan_for_check_ids(
        self,
        check_ids: tuple[str, ...],
    ) -> ActiveMigrationPlan:
        """Return the active migration coverage for one concrete check workset."""
        if self.artifact_path is None:
            return ActiveMigrationPlan()
        families_by_check_id = self.families_by_check_id
        families: list[MigrationFamily] = []
        missing_check_ids: list[str] = []
        for check_id in check_ids:
            family = families_by_check_id.get(check_id)
            if family is None:
                missing_check_ids.append(check_id)
                continue
            families.append(family)
        return ActiveMigrationPlan(
            families=tuple(families),
            missing_check_ids=tuple(missing_check_ids),
        )


def load_migration_catalog(
    *,
    artifact_path: Path | None,
    estimation_sheet_path: Path | None,
) -> MigrationCatalog:
    """Load the optional migration catalog from legacy inventory artifacts."""
    if artifact_path is None:
        return MigrationCatalog(
            artifact_path=None,
            estimation_sheet_path=estimation_sheet_path,
        )
    if not artifact_path.exists():
        raise FileNotFoundError(f"Legacy inventory artifact not found: {artifact_path}")

    artifact = _require_object(
        json.loads(artifact_path.read_text(encoding="utf-8")),
        context=str(artifact_path),
    )
    if artifact.get("version") != 2:
        raise RuntimeError(
            f"Unsupported legacy inventory artifact version in {artifact_path}."
        )

    estimation_by_check_id = _load_estimation_sheet(estimation_sheet_path)
    families: list[MigrationFamily] = []
    seen_check_ids: set[str] = set()
    raw_families = _require_list(
        artifact.get("families", []),
        context=f"{artifact_path} families",
    )
    for raw_family in raw_families:
        family_payload = _require_object(
            raw_family,
            context=f"{artifact_path} family record",
        )
        check_id = _required_string(
            family_payload,
            "check_id",
            context=str(artifact_path),
        )
        if check_id in seen_check_ids:
            raise RuntimeError(
                f"Legacy inventory artifact {artifact_path} contains duplicate family entries for {check_id}."
            )
        seen_check_ids.add(check_id)
        families.append(
            MigrationFamily(
                check_id=check_id,
                template_key=_required_string(
                    family_payload,
                    "template_key",
                    context=f"{artifact_path} family {check_id}",
                ),
                code_templates=_string_tuple(
                    family_payload.get("code_templates"),
                    context=f"{artifact_path} family {check_id} code_templates",
                ),
                placeholder_names=_string_tuple(
                    family_payload.get("placeholder_names"),
                    context=f"{artifact_path} family {check_id} placeholder_names",
                ),
                placeholder_count=_required_int(
                    family_payload,
                    "placeholder_count",
                    context=f"{artifact_path} family {check_id}",
                ),
                has_loop=_feature_bool(
                    family_payload,
                    "has_loop",
                    context=f"{artifact_path} family {check_id}",
                ),
                has_branching=_feature_bool(
                    family_payload,
                    "has_branching",
                    context=f"{artifact_path} family {check_id}",
                ),
                has_arithmetic=_feature_bool(
                    family_payload,
                    "has_arithmetic",
                    context=f"{artifact_path} family {check_id}",
                ),
                helper_calls=_feature_string_tuple(
                    family_payload,
                    "helper_calls",
                    context=f"{artifact_path} family {check_id}",
                ),
                source_files_count=_feature_int(
                    family_payload,
                    "source_files_count",
                    context=f"{artifact_path} family {check_id}",
                ),
                source_subroutines_count=_feature_int(
                    family_payload,
                    "source_subroutines_count",
                    context=f"{artifact_path} family {check_id}",
                ),
                unsupported_data_quality_emission_count_total=_feature_int(
                    family_payload,
                    "unsupported_data_quality_emission_count_total",
                    context=f"{artifact_path} family {check_id}",
                ),
                line_span_max=_feature_int(
                    family_payload,
                    "line_span_max",
                    context=f"{artifact_path} family {check_id}",
                ),
                statement_count_max=_feature_int(
                    family_payload,
                    "statement_count_max",
                    context=f"{artifact_path} family {check_id}",
                ),
                assessment=estimation_by_check_id.get(check_id),
            )
        )

    unknown_estimation_ids = sorted(set(estimation_by_check_id) - seen_check_ids)
    if unknown_estimation_ids:
        unknown_csv = ", ".join(unknown_estimation_ids)
        raise RuntimeError(
            f"Estimation sheet contains unknown check_id values: {unknown_csv}."
        )

    return MigrationCatalog(
        families=tuple(families),
        artifact_path=artifact_path.resolve(),
        estimation_sheet_path=(
            estimation_sheet_path.resolve()
            if estimation_sheet_path is not None
            else None
        ),
    )


def _load_estimation_sheet(
    path: Path | None,
) -> dict[str, MigrationAssessment]:
    """Load the optional flat estimation sheet."""
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"Legacy estimation sheet not found: {path}")
    with path.open(encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        fieldnames = set(reader.fieldnames or ())
        missing_columns = sorted(_REQUIRED_ESTIMATION_COLUMNS - fieldnames)
        if missing_columns:
            raise RuntimeError(
                f"{path} is missing estimation sheet columns: {', '.join(missing_columns)}."
            )

        assessments_by_check_id: dict[str, MigrationAssessment] = {}
        for row in reader:
            check_id = str(row.get("check_id", "")).strip()
            if not check_id:
                raise RuntimeError(f"{path} contains a row with an empty check_id.")
            if check_id in assessments_by_check_id:
                raise RuntimeError(f"{path} contains duplicate rows for {check_id}.")
            assessments_by_check_id[check_id] = MigrationAssessment(
                target_impl=cast(
                    "PlanningTargetImpl | None",
                    _optional_enum(
                        row.get("target_impl"),
                        field="target_impl",
                        allowed_values=_TARGET_IMPL_VALUES,
                        context=f"{path} row {check_id}",
                    ),
                ),
                size=cast(
                    "PlanningSize | None",
                    _optional_enum(
                        row.get("size"),
                        field="size",
                        allowed_values=_SIZE_VALUES,
                        context=f"{path} row {check_id}",
                    ),
                ),
                risk=cast(
                    "PlanningRisk | None",
                    _optional_enum(
                        row.get("risk"),
                        field="risk",
                        allowed_values=_RISK_VALUES,
                        context=f"{path} row {check_id}",
                    ),
                ),
                estimated_hours=_optional_string(row.get("estimated_hours")),
                rationale=_optional_string(row.get("rationale")),
            )
        return assessments_by_check_id


def _required_string(payload: dict[str, Any], field: str, *, context: str) -> str:
    """Return one required non-empty string value."""
    raw_value = payload.get(field)
    if isinstance(raw_value, str) and raw_value.strip():
        return raw_value.strip()
    raise RuntimeError(f"{context} must define a non-empty {field}.")


def _required_int(payload: dict[str, Any], field: str, *, context: str) -> int:
    """Return one required integer value."""
    raw_value = payload.get(field)
    if isinstance(raw_value, int):
        return raw_value
    raise RuntimeError(f"{context} must define an integer {field}.")


def _feature_bool(payload: dict[str, Any], field: str, *, context: str) -> bool:
    """Return one boolean feature field from a family payload."""
    raw_features = _require_object(
        payload.get("features"),
        context=f"{context} features",
    )
    raw_value = raw_features.get(field)
    if isinstance(raw_value, bool):
        return raw_value
    raise RuntimeError(f"{context} features must define boolean {field}.")


def _feature_int(payload: dict[str, Any], field: str, *, context: str) -> int:
    """Return one integer feature field from a family payload."""
    raw_features = _require_object(
        payload.get("features"),
        context=f"{context} features",
    )
    raw_value = raw_features.get(field)
    if isinstance(raw_value, int):
        return raw_value
    raise RuntimeError(f"{context} features must define integer {field}.")


def _feature_string_tuple(
    payload: dict[str, Any],
    field: str,
    *,
    context: str,
) -> tuple[str, ...]:
    """Return one string-list feature field from a family payload."""
    raw_features = _require_object(
        payload.get("features"),
        context=f"{context} features",
    )
    return _string_tuple(raw_features.get(field), context=f"{context} features.{field}")


def _string_tuple(value: object, *, context: str) -> tuple[str, ...]:
    """Return one string tuple from a JSON array field."""
    if value is None:
        return ()
    values = _require_list(value, context=context)
    strings: list[str] = []
    for raw_value in values:
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise RuntimeError(f"{context} must contain only non-empty strings.")
        strings.append(raw_value.strip())
    return tuple(strings)


def _require_list(value: object, *, context: str) -> list[object]:
    """Return one array payload."""
    if isinstance(value, list):
        return cast("list[object]", value)
    raise RuntimeError(f"{context} must be a list.")


def _require_object(value: object, *, context: str) -> dict[str, Any]:
    """Return one mapping payload with string keys."""
    if isinstance(value, dict):
        return cast("dict[str, Any]", value)
    raise RuntimeError(f"{context} must be an object.")


def _optional_string(value: object) -> str | None:
    """Return one stripped optional string value."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError("Expected an optional string field in estimation data.")
    normalized = value.strip()
    return normalized or None


def _optional_enum(
    value: object,
    *,
    field: str,
    allowed_values: frozenset[str],
    context: str,
) -> str | None:
    """Return one optional enum value from the estimation sheet."""
    normalized = _optional_string(value)
    if normalized is None:
        return None
    if normalized in allowed_values:
        return normalized
    raise RuntimeError(f"{context} has invalid {field} value {normalized!r}.")
