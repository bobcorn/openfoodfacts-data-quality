from __future__ import annotations

from typing import TYPE_CHECKING
from zipfile import ZIP_DEFLATED, ZipFile

if TYPE_CHECKING:
    from pathlib import Path

JSON_EXPORT_ARCHIVE_FILENAME = "openfoodfacts-data-quality-json.zip"


def write_json_export_archive(
    *, output_dir: Path, artifact_paths: tuple[Path, ...]
) -> Path:
    """Write the convenience ZIP download that bundles the JSON artifacts."""
    output_path = output_dir / JSON_EXPORT_ARCHIVE_FILENAME
    with ZipFile(output_path, mode="w", compression=ZIP_DEFLATED) as archive:
        for artifact_path in artifact_paths:
            archive.write(artifact_path, arcname=artifact_path.name)
    return output_path
