from fastapi import FastAPI
from fastapi.testclient import TestClient

from trees_api.routes.downloads.router import (
    AuthenticatedUser,
    CreateDownloadRequest,
    get_authenticated_user,
    get_supabase_client,
    router,
)
from trees_api.integrations.supabase.client import ActiveDownloadRequestExistsError


class _FakeSupabase:
    def __init__(self, dataset_row):
        self.dataset_row = dataset_row
        self.calls = 0

    def get_dataset_with_items(self, dataset_id: int):
        if self.dataset_row and int(self.dataset_row["id"]) == int(dataset_id):
            return self.dataset_row
        return None

    def create_or_get_active_download_request(
        self,
        dataset_id: int,
        requested_by: str,
        requester_email: str,
        include_raw: bool,
        include_segmentation: bool,
    ):
        self.calls += 1
        return {
            "id": 123,
            "dataset_id": dataset_id,
            "requested_by": requested_by,
            "requester_email": requester_email,
            "include_raw": include_raw,
            "include_segmentation": include_segmentation,
            "status": "pending",
        }


def _build_client(fake_supabase: _FakeSupabase, user: AuthenticatedUser) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_supabase_client] = lambda: fake_supabase
    app.dependency_overrides[get_authenticated_user] = lambda: user
    return TestClient(app)


def test_public_dataset_allows_authenticated_user_request():
    fake = _FakeSupabase(
        {
            "id": 11,
            "user_id": "owner-1",
            "visibility": "public",
            "archived": False,
            "dataset_items": [{"id": 1}],
        }
    )
    user = AuthenticatedUser(id="requester-2", email="req@example.com")
    client = _build_client(fake, user)

    response = client.post(
        "/downloads",
        json=CreateDownloadRequest(
            dataset_id=11,
            include_raw=True,
            include_segmentation=False,
        ).model_dump(),
    )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "pending"
    assert fake.calls == 1


def test_private_dataset_denies_non_owner():
    fake = _FakeSupabase(
        {
            "id": 12,
            "user_id": "owner-1",
            "visibility": "private",
            "archived": False,
            "dataset_items": [{"id": 1}],
        }
    )
    user = AuthenticatedUser(id="other-user", email="other@example.com")
    client = _build_client(fake, user)

    response = client.post(
        "/downloads",
        json=CreateDownloadRequest(
            dataset_id=12,
            include_raw=True,
            include_segmentation=False,
        ).model_dump(),
    )
    assert response.status_code == 403
    assert fake.calls == 0


def test_active_request_conflict_maps_to_409():
    class _ConflictSupabase(_FakeSupabase):
        def create_or_get_active_download_request(
            self,
            dataset_id: int,
            requested_by: str,
            requester_email: str,
            include_raw: bool,
            include_segmentation: bool,
        ):
            raise ActiveDownloadRequestExistsError("already exists")

    fake = _ConflictSupabase(
        {
            "id": 13,
            "user_id": "owner-1",
            "visibility": "public",
            "archived": False,
            "dataset_items": [{"id": 1}],
        }
    )
    user = AuthenticatedUser(id="requester-3", email="req3@example.com")
    client = _build_client(fake, user)

    response = client.post(
        "/downloads",
        json=CreateDownloadRequest(
            dataset_id=13,
            include_raw=True,
            include_segmentation=False,
        ).model_dump(),
    )
    assert response.status_code == 409

