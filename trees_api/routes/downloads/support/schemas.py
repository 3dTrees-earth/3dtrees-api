from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict


class ArchiveInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    root_folder: str
    dataset_id: int
    generated_at_utc: str


class DatasetInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    uuid: Optional[str] = None
    title: str
    visibility: Optional[Literal["private", "public", "view_only", "restricted"]] = None
    archived: bool
    dataset_url: str


class RequestInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    download_request_id: int
    include_raw: bool
    include_segmentation: bool


class GalaxyWorkflowInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    invocation_id: Optional[str] = None
    workflow_name: Optional[str] = None
    status: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


class SegmentationInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    used_model: Optional[str] = None
    model_source: Optional[str] = None


class ItemProcessingInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_item_id: int
    segmentation_process_duration_minutes: Optional[float] = None
    standardization_process_duration_minutes: Optional[float] = None
    coordinate_reference: Optional[str] = None


class ProcessingInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    galaxy_workflow: GalaxyWorkflowInfo
    segmentation: SegmentationInfo
    items: List[ItemProcessingInfo]
    warnings: List[str] = []


class FileMappingEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_item_id: int
    artifact_type: Literal["raw", "segmentation"]
    archive_path: str
    source_bucket: str
    source_key: str
    original_filename: str


class AttributionInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project: str
    required_fields: List[str]
    dataset_id: int
    dataset_title: str
    access_date: str
    dataset_url: str


class CitationInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recommended: str


class DownloadArchiveMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str
    archive: ArchiveInfo
    dataset: DatasetInfo
    request: RequestInfo
    processing: ProcessingInfo
    files: List[FileMappingEntry]
    attribution: AttributionInfo
    citation: CitationInfo


__all__ = [
    "ArchiveInfo",
    "DatasetInfo",
    "RequestInfo",
    "GalaxyWorkflowInfo",
    "SegmentationInfo",
    "ItemProcessingInfo",
    "ProcessingInfo",
    "FileMappingEntry",
    "AttributionInfo",
    "CitationInfo",
    "DownloadArchiveMetadata",
]

