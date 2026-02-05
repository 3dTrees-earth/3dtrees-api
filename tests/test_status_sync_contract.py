from datetime import datetime, timedelta, timezone
from pathlib import Path

from trees_api.models import WorkflowInvocation, WorkflowName
from trees_api.status_sync import MISSING_INVOCATION_DISCARD_AFTER, sync_workflow_statuses


class DummyGalaxyClient:
    def __init__(self, workflow_structure, invocations, file_raises=None):
        self._workflow_structure = workflow_structure
        self._invocations = invocations
        self._file_raises = file_raises
        # Required attributes for logging
        self.workflows_path = Path("/mock/workflows")
        self.workflow_registry = {"EndToEndPipeline-GalaxyEU": "mock-uuid"}

    def get_workflow_structure(self, workflow_name: str):
        return self._workflow_structure

    def get_workflow_structure_from_file(self, workflow_name: str):
        if self._file_raises:
            raise self._file_raises
        return self._workflow_structure

    def get_workflow_invocations(self, invocation_ids=None):
        if invocation_ids is None:
            return list(self._invocations)
        return [inv for inv in self._invocations if inv.get("id") in invocation_ids]


class DummySupabaseClient:
    def __init__(self, invocations):
        self._invocations = invocations
        self.updated = {}

    def get_unfinished_workflow_invocations(self):
        return self._invocations

    def update_workflow_invocation(self, invocation_id: str, **updates):
        self.updated[invocation_id] = updates
        for inv in self._invocations:
            if inv.invocation_id == invocation_id:
                data = inv.model_dump()
                data.update(updates)
                return WorkflowInvocation.model_validate(data)
        raise AssertionError(f"Invocation {invocation_id} not found in stub")


def test_sync_does_not_complete_when_steps_missing():
    invocation_id = "inv-partial-1"
    supabase_inv = WorkflowInvocation(
        id=1,
        invocation_id=invocation_id,
        dataset_id=123,
        workflow_name=WorkflowName.ENDTOEND_GALAXY_EU,
        status="new",
        created_at=datetime.now(),
        steps=[],
        inputs={},
        outputs={},
        output_collections={},
        jobs=[],
        messages=[],
        parameters={},
    )

    workflow_structure = {
        "steps": {
            "1": {"uuid": "step-1", "tool_id": "tool-a", "type": "tool"},
            "2": {"uuid": "step-2", "tool_id": "tool-b", "type": "tool"},
        }
    }

    galaxy_inv = {
        "id": invocation_id,
        "state": "scheduled",
        "jobs": [{"id": "job-1", "state": "ok"}],
        "steps": [
            {
                "workflow_step_uuid": "step-1",
                "state": "ok",
                "job_id": "job-1",
            },
            {
                "workflow_step_uuid": "step-2",
                "state": "scheduled",
                "job_id": "job-2",
            },
        ],
        "inputs": {},
        "messages": [],
        "outputs": {},
        "output_collections": {},
    }

    galaxy_client = DummyGalaxyClient(workflow_structure, [galaxy_inv])
    supabase_client = DummySupabaseClient([supabase_inv])

    stats = sync_workflow_statuses(galaxy_client, supabase_client)
    assert stats["errors"] == 0
    assert invocation_id in supabase_client.updated

    update = supabase_client.updated[invocation_id]
    assert update["status"] == "scheduled"
    assert "finished_at" not in update


def test_missing_invocation_discarded_when_stale():
    invocation_id = "inv-missing-1"
    created_at = datetime.now(timezone.utc) - MISSING_INVOCATION_DISCARD_AFTER - timedelta(minutes=1)
    supabase_inv = WorkflowInvocation(
        id=2,
        invocation_id=invocation_id,
        dataset_id=None,
        workflow_name=WorkflowName.SEGMENTATION,
        status="new",
        created_at=created_at,
        steps=[],
        inputs={},
        outputs={},
        output_collections={},
        jobs=[],
        messages=[],
        parameters={},
    )

    galaxy_client = DummyGalaxyClient(workflow_structure={"steps": {}}, invocations=[])
    supabase_client = DummySupabaseClient([supabase_inv])

    stats = sync_workflow_statuses(galaxy_client, supabase_client)
    assert stats["errors"] == 0
    update = supabase_client.updated.get(invocation_id)
    assert update is not None
    assert update["status"] == "discarded"
    assert "finished_at" in update


def test_galaxy_eu_scenario_completes_with_invocation_fallback():
    """
    Contract test: Galaxy EU scenario where:
    - Galaxy API returns uuid=None for all workflow steps
    - File lookup fails (simulating container without .ga files)
    - But invocation steps have workflow_step_uuid and all jobs are ok
    
    The invocation-based fallback should correctly complete the workflow.
    This is the exact scenario from issue 3DT-621.
    """
    invocation_id = "707a630ece75c892"
    supabase_inv = WorkflowInvocation(
        id=3,
        invocation_id=invocation_id,
        dataset_id=300,
        workflow_name=WorkflowName.ENDTOEND_GALAXY_EU,
        status="scheduled",
        created_at=datetime.now(timezone.utc),
        steps=[],
        inputs={},
        outputs={},
        output_collections={},
        jobs=[],
        messages=[],
        parameters={},
    )

    # Galaxy API returns uuid=None for all steps (Galaxy EU behavior)
    workflow_structure = {
        "steps": {
            "1": {"uuid": None, "tool_id": "3dtrees_standardization", "type": "tool"},
            "2": {"uuid": None, "tool_id": "3dtrees_standardization", "type": "tool"},
            "3": {"uuid": None, "tool_id": "3dtrees_smart_tile", "type": "tool"},
        }
    }

    # All 3 jobs are ok, all steps have workflow_step_uuid
    galaxy_inv = {
        "id": invocation_id,
        "state": "scheduled",  # Galaxy EU never updates this
        "jobs": [
            {"id": "job-1", "state": "ok"},
            {"id": "job-2", "state": "ok"},
            {"id": "job-3", "state": "ok"},
        ],
        "steps": [
            {"workflow_step_uuid": "step-uuid-1", "state": "scheduled", "job_id": "job-1"},
            {"workflow_step_uuid": "step-uuid-2", "state": "scheduled", "job_id": "job-2"},
            {"workflow_step_uuid": "step-uuid-3", "state": "scheduled", "job_id": "job-3"},
        ],
        "inputs": {},
        "messages": [],
        "outputs": {},
        "output_collections": {},
    }

    # File lookup fails (simulating container without .ga files)
    galaxy_client = DummyGalaxyClient(
        workflow_structure=workflow_structure,
        invocations=[galaxy_inv],
        file_raises=FileNotFoundError("Workflow file not found"),
    )
    supabase_client = DummySupabaseClient([supabase_inv])

    stats = sync_workflow_statuses(galaxy_client, supabase_client)

    # Should complete without errors
    assert stats["errors"] == 0
    assert stats["status_updated"] == 1

    # Should mark as ok (all jobs terminal, fallback found step UUIDs)
    update = supabase_client.updated.get(invocation_id)
    assert update is not None
    assert update["status"] == "ok", f"Expected 'ok', got {update['status']}"
    assert "finished_at" in update
