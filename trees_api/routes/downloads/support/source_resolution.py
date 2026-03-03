from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from trees_api.integrations.storage.client import StorageClient


@dataclass(frozen=True)
class ArchiveSource:
    bucket: str
    key: str
    arcname: str


def archive_filename(dataset_id: int) -> str:
    return f"3dt_{dataset_id}.zip"


def archive_root(dataset_id: int) -> str:
    return f"3dt_{dataset_id}"


def artifact_base_name(dataset_id: int, dataset_item_id: int) -> str:
    return f"3dtree_{dataset_id}_{dataset_item_id}"


def _segmentation_candidates(dataset_id: int, dataset_item_id: int) -> List[str]:
    return [
        f"{dataset_id}/segmentation/{dataset_item_id}.laz",
        f"{dataset_id}/segmentation/{dataset_item_id}.las",
        f"{dataset_id}/segmentation/{dataset_item_id}/segmented.laz",
        f"{dataset_id}/segmentation/{dataset_item_id}/segmented.las",
    ]


def extract_storage_key_from_url(url: Optional[str], bucket_name: str) -> Optional[str]:
    """Extract an S3 key from common URL formats, scoped to a bucket."""
    if not url:
        return None

    value = url.strip()
    if not value:
        return None

    if value.startswith("s3://"):
        payload = value[len("s3://") :]
        bucket, _, key = payload.partition("/")
        if bucket == bucket_name and key:
            return key
        return None

    parsed = urlparse(value)
    if parsed.scheme and parsed.netloc:
        path = parsed.path.lstrip("/")
        if path.startswith(f"{bucket_name}/"):
            key = path[len(bucket_name) + 1 :]
            return key or None

        marker = f"/{bucket_name}/"
        marker_index = parsed.path.find(marker)
        if marker_index != -1:
            key = parsed.path[marker_index + len(marker) :]
            return key or None

        if parsed.netloc.startswith(f"{bucket_name}.") and path:
            return path

        return None

    plain = value.lstrip("/")
    if plain.startswith(f"{bucket_name}/"):
        return plain[len(bucket_name) + 1 :] or None
    return plain or None


def resolve_segmentation_key(
    storage: StorageClient,
    dataset_id: int,
    dataset_item_id: int,
    segmentation_row: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Resolve segmentation by DB URL first, then filesystem-pattern fallback."""
    if segmentation_row:
        db_url_key = extract_storage_key_from_url(
            segmentation_row.get("url"), storage.bucket_name_products
        )
        if db_url_key and storage.file_exists(
            db_url_key, bucket=storage.bucket_name_products
        ):
            return db_url_key

    for candidate in _segmentation_candidates(dataset_id, dataset_item_id):
        if storage.file_exists(candidate, bucket=storage.bucket_name_products):
            return candidate
    return None


def build_archive_sources(
    storage: StorageClient,
    dataset: Dict[str, Any],
    request_row: Dict[str, Any],
    archive_root_name: str,
    segmentation_rows_by_item: Optional[Dict[int, Dict[str, Any]]] = None,
) -> tuple[List[ArchiveSource], List[Dict[str, Any]], List[int], List[int]]:
    dataset_id = int(dataset["id"])
    include_raw = bool(request_row.get("include_raw"))
    include_segmentation = bool(request_row.get("include_segmentation"))
    items = dataset.get("dataset_items") or []
    segmentation_rows_by_item = segmentation_rows_by_item or {}

    sources: List[ArchiveSource] = []
    mapping_rows: List[Dict[str, Any]] = []
    missing_segmentation: List[int] = []
    missing_raw: List[int] = []

    for item in items:
        item_id = int(item["id"])
        original_name = item.get("file_name") or ""
        base_name = artifact_base_name(dataset_id, item_id)

        if include_raw:
            raw_key = item.get("bucket_path")
            if not raw_key:
                missing_raw.append(item_id)
            elif not storage.file_exists(raw_key, bucket=storage.bucket_name_raw):
                missing_raw.append(item_id)
            else:
                raw_ext = Path(raw_key).suffix or Path(original_name).suffix or ".laz"
                raw_arcname = (
                    f"{archive_root_name}/data/raw/{base_name}_raw{raw_ext}"
                )
                sources.append(
                    ArchiveSource(
                        bucket=storage.bucket_name_raw,
                        key=raw_key,
                        arcname=raw_arcname,
                    )
                )
                mapping_rows.append(
                    {
                        "dataset_item_id": item_id,
                        "artifact_type": "raw",
                        "archive_path": raw_arcname,
                        "source_bucket": storage.bucket_name_raw,
                        "source_key": raw_key,
                        "original_filename": original_name,
                    }
                )

        if include_segmentation:
            seg_key = resolve_segmentation_key(
                storage=storage,
                dataset_id=dataset_id,
                dataset_item_id=item_id,
                segmentation_row=segmentation_rows_by_item.get(item_id),
            )
            if not seg_key:
                missing_segmentation.append(item_id)
            else:
                seg_ext = Path(seg_key).suffix or ".laz"
                seg_arcname = (
                    f"{archive_root_name}/data/segmentation/"
                    f"{base_name}_segmentation{seg_ext}"
                )
                sources.append(
                    ArchiveSource(
                        bucket=storage.bucket_name_products,
                        key=seg_key,
                        arcname=seg_arcname,
                    )
                )
                mapping_rows.append(
                    {
                        "dataset_item_id": item_id,
                        "artifact_type": "segmentation",
                        "archive_path": seg_arcname,
                        "source_bucket": storage.bucket_name_products,
                        "source_key": seg_key,
                        "original_filename": original_name,
                    }
                )

    return sources, mapping_rows, missing_segmentation, missing_raw


__all__ = [
    "ArchiveSource",
    "archive_filename",
    "archive_root",
    "artifact_base_name",
    "extract_storage_key_from_url",
    "resolve_segmentation_key",
    "build_archive_sources",
]

