from types import SimpleNamespace

from trees_api.status_sync import (
    _all_expected_steps_terminal,
    _build_step_terminal_map,
    _determine_workflow_completion,
    _extract_outputs_from_jobs,
    _get_expected_tool_step_uuids,
    _map_galaxy_status,
    _update_steps_from_jobs,
)


class DummyJobsClient:
    def __init__(self, job):
        self._job = job

    def get(self, job_id):
        return self._job


class DummyGalaxyClient:
    def __init__(self, workflow_structure=None, job=None):
        self._workflow_structure = workflow_structure or {}
        if job is not None:
            self.gi = SimpleNamespace(jobs=DummyJobsClient(job))

    def get_workflow_structure(self, name):
        return self._workflow_structure


def test_map_galaxy_status_unknown_defaults_to_ready():
    assert _map_galaxy_status("totally-new-state") == "ready"


def test_map_galaxy_status_mapping():
    assert _map_galaxy_status("requires_materialization") == "ready"


def test_build_step_terminal_map_respects_job_state():
    galaxy_inv = {
        "steps": [
            {
                "workflow_step_uuid": "step-1",
                "state": "scheduled",
                "job_id": "job-1",
            }
        ]
    }
    jobs = [{"id": "job-1", "state": "ok"}]
    step_terminal = _build_step_terminal_map(galaxy_inv, jobs)
    assert step_terminal["step-1"] is True


def test_build_step_terminal_map_respects_step_jobs():
    galaxy_inv = {
        "steps": [
            {
                "workflow_step_uuid": "step-1",
                "state": "scheduled",
                "jobs": [{"state": "ok"}, {"state": "running"}],
            }
        ]
    }
    step_terminal = _build_step_terminal_map(galaxy_inv, jobs=[])
    assert step_terminal["step-1"] is False


def test_all_expected_steps_terminal_requires_non_empty_set():
    assert _all_expected_steps_terminal(set(), {"step-1": True}) is False


def test_determine_workflow_completion_requires_all_expected_steps():
    jobs = [{"id": "job-1", "state": "ok"}]
    expected_steps = {"step-1", "step-2"}
    step_terminal_map = {"step-1": True, "step-2": False}
    finished, final_status, all_steps_terminal = _determine_workflow_completion(
        "scheduled",
        jobs,
        expected_steps,
        step_terminal_map,
        "inv-123",
    )
    assert finished is False
    assert final_status is None
    assert all_steps_terminal is False


def test_determine_workflow_completion_ok_when_jobs_and_steps_terminal():
    jobs = [{"id": "job-1", "state": "ok"}, {"id": "job-2", "state": "ok"}]
    expected_steps = {"step-1"}
    step_terminal_map = {"step-1": True}
    finished, final_status, all_steps_terminal = _determine_workflow_completion(
        "scheduled",
        jobs,
        expected_steps,
        step_terminal_map,
        "inv-123",
    )
    assert finished is True
    assert final_status == "ok"
    assert all_steps_terminal is True


def test_determine_workflow_completion_error_on_failed_jobs():
    jobs = [{"id": "job-1", "state": "error"}]
    expected_steps = {"step-1"}
    step_terminal_map = {"step-1": True}
    finished, final_status, all_steps_terminal = _determine_workflow_completion(
        "scheduled",
        jobs,
        expected_steps,
        step_terminal_map,
        "inv-123",
    )
    assert finished is True
    assert final_status == "error"
    assert all_steps_terminal is True


def test_determine_workflow_completion_terminal_galaxy_state():
    finished, final_status, all_steps_terminal = _determine_workflow_completion(
        "ok",
        jobs=[],
        expected_step_uuids=set(),
        step_terminal_map={},
        invocation_id="inv-123",
    )
    assert finished is True
    assert final_status == "ok"
    assert all_steps_terminal is False


def test_update_steps_from_jobs_overrides_state():
    steps = [{"job_id": "job-1", "state": "running"}]
    jobs = [{"id": "job-1", "state": "ok"}]
    updated = _update_steps_from_jobs(steps, jobs)
    assert updated[0]["state"] == "ok"


def test_get_expected_tool_step_uuids_filters_non_tool_steps():
    workflow_structure = {
        "steps": {
            "1": {"uuid": "uuid-1", "tool_id": "tool-x", "type": "tool"},
            "2": {"uuid": "uuid-2", "tool_id": None, "type": "tool"},
            "3": {"uuid": "uuid-3", "tool_id": "tool-y", "type": "data_input"},
            "4": {"uuid": "uuid-4", "tool_id": "tool-z", "type": "tool"},
        }
    }
    galaxy_client = DummyGalaxyClient(workflow_structure=workflow_structure)
    cache = {}
    expected = _get_expected_tool_step_uuids(galaxy_client, "AnyWorkflow", cache)
    assert expected == {"uuid-1", "uuid-4"}


def test_extract_outputs_from_jobs_uses_first_ok_job():
    job = SimpleNamespace(outputs={"out": {"id": "dataset-1"}}, output_collections={})
    galaxy_client = DummyGalaxyClient(job=job)
    outputs, collections = _extract_outputs_from_jobs(
        galaxy_client,
        jobs=[{"id": "job-1", "state": "ok"}],
    )
    assert outputs == {"out": {"id": "dataset-1"}}
    assert collections == {}
