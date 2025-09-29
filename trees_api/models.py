from typing import Optional, Union
from datetime import datetime
from enum import StrEnum

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
    visibility: Optional[str] = None

class WorkflowName(StrEnum):
    OVERVIEW = "Overviews"
    SEGMENTATION = "Segmentation"

# WorkflowStatus enum removed - using Galaxy states directly in database

class WorkflowInvocation(BaseModel):
    id: int
    invocation_id: str
    dataset_id: int
    workflow_name: WorkflowName
    status: str = "new"  # Galaxy state - no enum needed
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    
    # Separate JSONB fields for efficient comparison and updates
    steps: list = []
    inputs: list = []
    outputs: dict = {}
    output_collections: dict = {}
    jobs: list = []
    messages: list = []
    parameters: dict = {}  # User-defined parameters for the workflow
    results_synced: bool = False
    results_synced_at: Optional[datetime] = None

    def has_jobs_changed(self, other_jobs: list) -> bool:
        """Check if jobs have changed by comparing length and job states"""
        if len(self.jobs) != len(other_jobs):
            return True
        
        # Compare job states
        for i, job in enumerate(self.jobs):
            if i >= len(other_jobs):
                return True
            if job.get('state') != other_jobs[i].get('state'):
                return True
        
        return False
    
    def has_messages_changed(self, other_messages: list) -> bool:
        """Check if messages have changed by comparing length"""
        return len(self.messages) != len(other_messages)
    
    def has_outputs_changed(self, other_outputs: dict) -> bool:
        """Check if outputs have changed"""
        return self.outputs != other_outputs
    
    def has_output_collections_changed(self, other_collections: dict) -> bool:
        """Check if output collections have changed"""
        return self.output_collections != other_collections


