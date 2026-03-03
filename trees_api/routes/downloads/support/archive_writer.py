from __future__ import annotations

import json
import zipfile
from pathlib import Path
from typing import Dict, List

from trees_api.routes.downloads.support.source_resolution import ArchiveSource
from trees_api.integrations.storage.client import StorageClient


def write_download_archive(
    storage: StorageClient,
    archive_local_path: Path,
    archive_root_name: str,
    readme_text: str,
    license_text: str,
    metadata_payload: Dict[str, object],
    sources: List[ArchiveSource],
) -> int:
    """
    Build a zip archive from staged object-store files and metadata.

    Returns:
        Archive size in bytes.
    """
    work_path = archive_local_path.parent
    staged_file_path = work_path / "staged.bin"

    with zipfile.ZipFile(
        archive_local_path,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        allowZip64=True,
    ) as zip_file:
        zip_file.writestr(f"{archive_root_name}/README.md", readme_text)
        zip_file.writestr(f"{archive_root_name}/LICENSE.txt", license_text)
        zip_file.writestr(
            f"{archive_root_name}/metadata.json",
            json.dumps(metadata_payload, indent=2),
        )

        for source in sources:
            if staged_file_path.exists():
                staged_file_path.unlink()
            storage.download_file(
                key=source.key,
                file_path=staged_file_path,
                bucket=source.bucket,
            )
            zip_file.write(staged_file_path, arcname=source.arcname)
            staged_file_path.unlink(missing_ok=True)

    return archive_local_path.stat().st_size


__all__ = ["write_download_archive"]

