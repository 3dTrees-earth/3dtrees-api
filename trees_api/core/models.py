from datetime import datetime
from enum import StrEnum
from typing import Literal, Optional, Union

from pydantic import BaseModel


class Dataset(BaseModel):
    id: Optional[int]
    uuid: str
    title: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    user_id: str
    acquisition_date: datetime
    bucket_path: str
    file_name: Optional[str] = None
    visibility: Optional[Literal["private", "public", "view_only", "restricted"]] = None


class WorkflowName(StrEnum):
    STANDARD = "Standard"
    OVERVIEW = "Overviews"
    SEGMENTATION = "Segmentation"
    PY3DTILES = "Py3DTiles"
    ENDTOEND = "EndToEndPipeline"
    ENDTOEND_GALAXY_EU = "EndToEndPipeline-GalaxyEU"


class WorkflowInvocation(BaseModel):
    id: int
    invocation_id: str
    dataset_id: Optional[int] = None
    workflow_name: WorkflowName
    status: str = "new"
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    steps: list = []
    inputs: Union[dict, list] = {}
    outputs: dict = {}
    output_collections: dict = {}
    jobs: list = []
    messages: list = []
    parameters: dict = {}
    results_synced: bool = False
    results_synced_at: Optional[datetime] = None
    metadata_synced_at: Optional[datetime] = None

    def has_jobs_changed(self, other_jobs: list) -> bool:
        if len(self.jobs) != len(other_jobs):
            return True
        for idx, job in enumerate(self.jobs):
            if idx >= len(other_jobs):
                return True
            if job.get("state") != other_jobs[idx].get("state"):
                return True
        return False

    def has_messages_changed(self, other_messages: list) -> bool:
        return len(self.messages) != len(other_messages)

    def has_outputs_changed(self, other_outputs: dict) -> bool:
        return self.outputs != other_outputs

    def has_output_collections_changed(self, other_collections: dict) -> bool:
        return self.output_collections != other_collections

