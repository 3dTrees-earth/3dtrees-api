from typing import Optional
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel



class WorkflowName(StrEnum):
    OVERVIEW = "overviews"
    STANDARDIZATION = "standardization"

class WorkflowStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESSFUL = "successful"
    WARNING = "warning"
    ERRORED = "errored"

class WorkflowInvocation(BaseModel):
    id: int
    invocation_id: str
    dataset_id: int
    workflow_name: WorkflowName
    status: WorkflowStatus = WorkflowStatus.PENDING
    payload: dict = {}
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    def __eq__(self, other: 'WorkflowInvocation' | dict) -> bool:
        """Compare two WorkflowInvocation or payload dicts to check for updates"""
        if not isinstance(other, dict):
            other = other.payload
        
        return self.payload == other

class CreateWorkflowInvocation(BaseModel):
    dataset_id: int
    workflow_name: WorkflowName
    payload: dict = {}

