from __future__ import annotations

from functools import cache
from importlib import import_module
from pkgutil import iter_modules
from typing import TYPE_CHECKING

from openfoodfacts_data_quality.checks.dsl.resources import dsl_check_pack_resources

if TYPE_CHECKING:
    from importlib.resources.abc import Traversable


@cache
def default_python_check_pack_module_names() -> tuple[str, ...]:
    """Return the import paths for the packaged Python check packs."""
    package = import_module("openfoodfacts_data_quality.checks.packs.python")
    search_locations = getattr(package, "__path__", None)
    if search_locations is None:
        return ()
    return tuple(
        f"{package.__name__}.{module_info.name}"
        for module_info in sorted(
            iter_modules(search_locations), key=lambda item: item.name
        )
        if not module_info.ispkg and not module_info.name.startswith("_")
    )


@cache
def default_dsl_check_pack_resources() -> tuple[Traversable, ...]:
    """Return the packaged DSL check-pack files shipped with the library."""
    return dsl_check_pack_resources()
