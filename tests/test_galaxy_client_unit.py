from pathlib import Path
from types import SimpleNamespace

from trees_api.core.config import GalaxyConfig
from trees_api.integrations.galaxy.client import GalaxyClient


class _FakeHistoryObjectsClient:
    def __init__(self):
        self.create_calls = []

    def create(self, name: str):
        self.create_calls.append(name)
        return SimpleNamespace(id="history-123", name=name)


class _FakeHistoryApiClient:
    def __init__(self):
        self.update_calls = []

    def update_history(self, history_id: str, **kwargs):
        self.update_calls.append((history_id, kwargs))
        return {"id": history_id, **kwargs}


def test_create_history_sets_preferred_object_store_when_requested():
    client = GalaxyClient(
        GalaxyConfig(url="https://example.org", workflows_path=Path("."))
    )
    history_objects = _FakeHistoryObjectsClient()
    history_api = _FakeHistoryApiClient()
    client.gi = SimpleNamespace(
        histories=history_objects,
        gi=SimpleNamespace(histories=history_api),
    )

    history = client.create_history(
        "Scratch History",
        preferred_object_store_id="s3_scratch_netapp01",
    )

    assert history.id == "history-123"
    assert history_objects.create_calls == ["Scratch History"]
    assert history_api.update_calls == [
        (
            "history-123",
            {"preferred_object_store_id": "s3_scratch_netapp01"},
        )
    ]
