from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from trees_api.routes.downloads.support.schemas import (
    ArchiveInfo,
    AttributionInfo,
    CitationInfo,
    DatasetInfo,
    DownloadArchiveMetadata,
    FileMappingEntry,
    GalaxyWorkflowInfo,
    ItemProcessingInfo,
    ProcessingInfo,
    RequestInfo,
    SegmentationInfo,
)


def extract_model_from_parameters(
    node: Any,
    path: str = "",
) -> Tuple[Optional[str], Optional[str]]:
    candidate_keys = {
        "segmentation_model",
        "model",
        "model_name",
        "model_id",
        "model_path",
        "checkpoint",
        "checkpoint_path",
        "weights",
        "weights_path",
        "ckpt",
    }

    if isinstance(node, dict):
        for key, value in node.items():
            key_lower = str(key).lower()
            child_path = f"{path}.{key}" if path else str(key)
            if isinstance(value, (str, int, float)) and not isinstance(value, bool):
                if (
                    key_lower in candidate_keys
                    or "model" in key_lower
                    or "checkpoint" in key_lower
                ):
                    value_str = str(value).strip()
                    if value_str:
                        return value_str, child_path
            model_value, source = extract_model_from_parameters(value, child_path)
            if model_value:
                return model_value, source
    elif isinstance(node, list):
        for idx, value in enumerate(node):
            child_path = f"{path}[{idx}]" if path else f"[{idx}]"
            model_value, source = extract_model_from_parameters(value, child_path)
            if model_value:
                return model_value, source

    return None, None


def build_readme(
    dataset: Dict[str, Any],
    request_row: Dict[str, Any],
    archive_file_name: str,
    segmentation_model: Optional[str],
    workflow_name: Optional[str],
) -> str:
    title = dataset.get("title") or f"Dataset {dataset.get('id')}"
    created = datetime.now(timezone.utc).isoformat()
    dataset_id = dataset.get("id")
    include_raw = bool(request_row.get("include_raw"))
    include_segmentation = bool(request_row.get("include_segmentation"))
    raw_line = "yes" if include_raw else "no"
    segmentation_line = "yes" if include_segmentation else "no"
    model_line = segmentation_model or "not available in current DB metadata"
    workflow_line = workflow_name or "not available"
    return (
        "# 3Dtrees Dataset Download\n\n"
        "## Dataset\n"
        f"- Dataset ID: {dataset_id}\n"
        f"- Title: {title}\n"
        f"- Download request ID: {request_row.get('id')}\n"
        f"- Generated at (UTC): {created}\n"
        f"- Includes raw data: {raw_line}\n"
        f"- Includes segmentation data: {segmentation_line}\n\n"
        "## Processing summary\n"
        f"- Galaxy workflow: {workflow_line}\n"
        f"- Segmentation model used: {model_line}\n\n"
        "## Archive structure\n"
        "- data/raw/: raw files if requested\n"
        "- data/segmentation/: segmentation files if requested\n"
        "- metadata.json: machine-readable metadata and filename mapping\n"
        "- LICENSE.txt: license notice, attribution, and citation guidance\n\n"
        "## Naming convention\n"
        f"- Archive filename: {archive_file_name}\n"
        "- Raw file: 3dtree_{dataset_id}_{item_id}_raw.{ext}\n"
        "- Segmentation file: 3dtree_{dataset_id}_{item_id}_segmentation.{ext}\n\n"
        "## Dataset page\n"
        f"- https://3dtrees.earth/datasets/{dataset_id}\n"
    )


def build_license_note(dataset: Dict[str, Any], segmentation_model: Optional[str]) -> str:
    dataset_id = dataset.get("id")
    title = dataset.get("title") or f"Dataset {dataset_id}"
    year = datetime.now(timezone.utc).year
    access_date = datetime.now(timezone.utc).date().isoformat()
    model_line = segmentation_model or "not available in current DB metadata"
    return (
        "3Dtrees License, Citation, and Attribution\n"
        "==========================================\n\n"
        "License note\n"
        "This archive is distributed through 3Dtrees. Dataset-specific legal terms can vary.\n"
        "Use the dataset page as the authoritative source for rights and restrictions.\n\n"
        "Attribution\n"
        "When reusing this data, include at minimum:\n"
        "- Project: 3Dtrees\n"
        f"- Dataset ID: {dataset_id}\n"
        f"- Dataset title: {title}\n"
        f"- Access date: {access_date}\n"
        f"- URL: https://3dtrees.earth/datasets/{dataset_id}\n\n"
        "Segmentation model note\n"
        f"- Model used (if available): {model_line}\n\n"
        "Suggested citation\n"
        f'3Dtrees ({year}). "{title}" (Dataset ID: {dataset_id}). '
        f"3dtrees.earth. Accessed {access_date}.\n"
    )


def build_metadata_model(
    dataset: Dict[str, Any],
    request_row: Dict[str, Any],
    archive_file_name: str,
    archive_root_name: str,
    mapping_rows: List[Dict[str, Any]],
    generated_at: datetime,
    invocation_row: Optional[Dict[str, Any]],
    segmentation_rows_by_item: Dict[int, Dict[str, Any]],
    standardization_rows_by_item: Dict[int, Dict[str, Any]],
    warnings: Optional[List[str]] = None,
) -> DownloadArchiveMetadata:
    dataset_id = int(dataset["id"])
    title = dataset.get("title") or f"Dataset {dataset_id}"
    year = generated_at.year
    access_date = generated_at.date().isoformat()
    invocation_parameters = (invocation_row or {}).get("parameters") or {}
    used_model, model_source = extract_model_from_parameters(invocation_parameters)

    processing_items: List[ItemProcessingInfo] = []
    for item in dataset.get("dataset_items") or []:
        item_id = int(item["id"])
        seg_row = segmentation_rows_by_item.get(item_id) or {}
        std_row = standardization_rows_by_item.get(item_id) or {}
        processing_items.append(
            ItemProcessingInfo(
                dataset_item_id=item_id,
                segmentation_process_duration_minutes=seg_row.get(
                    "segmentation_process_duration_minutes"
                ),
                standardization_process_duration_minutes=std_row.get(
                    "standard_process_duration_minutes"
                ),
                coordinate_reference=std_row.get("coordinate_reference"),
            )
        )

    return DownloadArchiveMetadata(
        schema_version="1.0.0",
        archive=ArchiveInfo(
            name=archive_file_name,
            root_folder=archive_root_name,
            dataset_id=dataset_id,
            generated_at_utc=generated_at.isoformat(),
        ),
        dataset=DatasetInfo(
            id=dataset_id,
            uuid=dataset.get("uuid"),
            title=title,
            visibility=dataset.get("visibility"),
            archived=bool(dataset.get("archived")),
            dataset_url=f"https://3dtrees.earth/datasets/{dataset_id}",
        ),
        request=RequestInfo(
            download_request_id=int(request_row["id"]),
            include_raw=bool(request_row.get("include_raw")),
            include_segmentation=bool(request_row.get("include_segmentation")),
        ),
        processing=ProcessingInfo(
            galaxy_workflow=GalaxyWorkflowInfo(
                invocation_id=(invocation_row or {}).get("invocation_id"),
                workflow_name=(invocation_row or {}).get("workflow_name"),
                status=(invocation_row or {}).get("status"),
                started_at=(invocation_row or {}).get("started_at"),
                finished_at=(invocation_row or {}).get("finished_at"),
            ),
            segmentation=SegmentationInfo(
                used_model=used_model,
                model_source=model_source,
            ),
            items=processing_items,
            warnings=warnings or [],
        ),
        files=[FileMappingEntry.model_validate(row) for row in mapping_rows],
        attribution=AttributionInfo(
            project="3Dtrees",
            required_fields=[
                "project",
                "dataset_id",
                "dataset_title",
                "access_date",
                "dataset_url",
            ],
            dataset_id=dataset_id,
            dataset_title=title,
            access_date=access_date,
            dataset_url=f"https://3dtrees.earth/datasets/{dataset_id}",
        ),
        citation=CitationInfo(
            recommended=(
                f'3Dtrees ({year}). "{title}" (Dataset ID: {dataset_id}). '
                f"3dtrees.earth. Accessed {access_date}."
            )
        ),
    )


__all__ = [
    "extract_model_from_parameters",
    "build_readme",
    "build_license_note",
    "build_metadata_model",
]

