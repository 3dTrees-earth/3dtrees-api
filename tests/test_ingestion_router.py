from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

import trees_api.routes.ingestions.router as ingestion_router_module
from trees_api.routes.ingestions.router import (
    AuthenticatedUser,
    CreateIngestionRequest,
    IngestionItemCreateInput,
    PresignPartsRequest,
    CompleteIngestionRequest,
    CompleteIngestionItemRequest,
    CompletePart,
    get_authenticated_user,
    get_galaxy_client,
    get_storage_client,
    get_supabase_client,
    get_uploader_storage,
    router,
)


class _FakeUploadClient:
    def __init__(self):
        self.created = []
        self.completed = []

    def create_multipart_upload(self, Bucket, Key, ContentType):
        self.created.append((Bucket, Key, ContentType))
        return {"UploadId": f"upload-{len(self.created)}"}

    def generate_presigned_url(self, operation, Params, ExpiresIn):
        assert operation == "upload_part"
        return f"https://example.test/{Params['UploadId']}/{Params['PartNumber']}"

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        self.completed.append((Bucket, Key, UploadId, MultipartUpload))
        return {"ETag": "etag-value"}

    def head_object(self, Bucket, Key):
        return {"Bucket": Bucket, "Key": Key}


class _FakeUploaderStorage:
    def __init__(self):
        self.bucket_name_raw = "3dtrees-raw"
        self.client = _FakeUploadClient()


class _FakeSupabase:
    def __init__(self):
        self.sessions = {}
        self.items = {}
        self.dataset_items = {
            111: {"id": 111, "dataset_id": 10, "file_name": "tile_a.laz", "bucket_path": ""},
            112: {"id": 112, "dataset_id": 10, "file_name": "tile_b.las", "bucket_path": ""},
        }
        self.next_session_id = 1
        self.next_item_id = 1

    def get_dataset_with_items(self, dataset_id: int):
        if dataset_id != 10:
            return None
        return {
            "id": 10,
            "user_id": "owner-1",
            "visibility": "private",
            "archived": False,
            "dataset_items": [self.dataset_items[111], self.dataset_items[112]],
        }

    def create_or_get_active_ingestion_session(
        self,
        dataset_id: int,
        created_by: str,
        idempotency_key: str | None,
        workflow_name: str | None = None,
        metadata: dict | None = None,
    ):
        for session in self.sessions.values():
            if (
                session["dataset_id"] == dataset_id
                and session["created_by"] == created_by
                and session["status"] in {"pending", "uploading", "finalizing"}
            ):
                if idempotency_key is None or session.get("idempotency_key") == idempotency_key:
                    return session
        session_id = self.next_session_id
        self.next_session_id += 1
        session = {
            "id": session_id,
            "dataset_id": dataset_id,
            "created_by": created_by,
            "idempotency_key": idempotency_key,
            "workflow_name": workflow_name,
            "status": "pending",
            "metadata": metadata or {},
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.sessions[session_id] = session
        return session

    def update_ingestion_session(self, session_id: int, **updates):
        session = self.sessions.get(session_id)
        if not session:
            return None
        session.update(updates)
        session["updated_at"] = datetime.now(timezone.utc).isoformat()
        return session

    def list_ingestion_session_items(self, session_id: int):
        return [row for row in self.items.values() if row["ingestion_session_id"] == session_id]

    def create_or_update_ingestion_session_item(
        self,
        session_id: int,
        dataset_item_id: int,
        file_name: str,
        file_size_bytes: int,
        content_type: str,
        key: str,
        upload_id: str,
        status: str = "uploading",
    ):
        for row in self.items.values():
            if row["ingestion_session_id"] == session_id and row["dataset_item_id"] == dataset_item_id:
                row.update(
                    {
                        "file_name": file_name,
                        "file_size_bytes": file_size_bytes,
                        "content_type": content_type,
                        "s3_key": key,
                        "upload_id": upload_id,
                        "status": status,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                return row
        item_id = self.next_item_id
        self.next_item_id += 1
        row = {
            "id": item_id,
            "ingestion_session_id": session_id,
            "dataset_item_id": dataset_item_id,
            "file_name": file_name,
            "file_size_bytes": file_size_bytes,
            "content_type": content_type,
            "s3_key": key,
            "upload_id": upload_id,
            "status": status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.items[item_id] = row
        return row

    def get_ingestion_session_for_user(self, session_id: int, created_by: str):
        session = self.sessions.get(session_id)
        if not session:
            return None
        if session["created_by"] != created_by:
            return None
        return session

    def get_ingestion_session_item(self, session_id: int, ingestion_item_id: int):
        row = self.items.get(ingestion_item_id)
        if not row:
            return None
        if row["ingestion_session_id"] != session_id:
            return None
        return row

    def update_dataset_item_bucket_path_by_id(self, dataset_item_id: int, bucket_path: str):
        if dataset_item_id not in self.dataset_items:
            return None
        self.dataset_items[dataset_item_id]["bucket_path"] = bucket_path
        return self.dataset_items[dataset_item_id]

    def update_ingestion_session_item(self, session_id: int, ingestion_item_id: int, **updates):
        row = self.get_ingestion_session_item(session_id, ingestion_item_id)
        if not row:
            return None
        row.update(updates)
        row["updated_at"] = datetime.now(timezone.utc).isoformat()
        return row

    def list_ingestion_sessions_for_user(
        self,
        created_by: str,
        dataset_id: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ):
        rows = [s for s in self.sessions.values() if s["created_by"] == created_by]
        if dataset_id is not None:
            rows = [s for s in rows if s["dataset_id"] == dataset_id]
        rows.sort(key=lambda r: r["id"], reverse=True)
        return rows[offset : offset + limit]


def _build_client(fake_supabase: _FakeSupabase, fake_storage: _FakeUploaderStorage) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_supabase_client] = lambda: fake_supabase
    app.dependency_overrides[get_uploader_storage] = lambda: fake_storage
    app.dependency_overrides[get_authenticated_user] = lambda: AuthenticatedUser(
        id="owner-1",
        email="owner@example.com",
    )
    app.dependency_overrides[get_galaxy_client] = lambda: object()
    app.dependency_overrides[get_storage_client] = lambda: object()
    return TestClient(app)


def test_create_ingestion_returns_session_and_upload_plan():
    fake_supabase = _FakeSupabase()
    fake_storage = _FakeUploaderStorage()
    client = _build_client(fake_supabase, fake_storage)

    response = client.post(
        "/ingestions",
        headers={"Idempotency-Key": "abc-123"},
        json=CreateIngestionRequest(
            dataset_id=10,
            items=[
                IngestionItemCreateInput(
                    dataset_item_id=111,
                    file_name="tile_a.laz",
                    file_size_bytes=2048,
                ),
                IngestionItemCreateInput(
                    dataset_item_id=112,
                    file_name="tile_b.las",
                    file_size_bytes=4096,
                ),
            ],
            metadata={"source": "test"},
            workflow_name="EndToEndPipeline",
        ).model_dump(),
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["status"] == "uploading"
    assert data["dataset_id"] == 10
    assert len(data["items"]) == 2
    assert data["items"][0]["s3_key"] == "RAW/10/111/raw.laz"
    assert data["items"][1]["s3_key"] == "RAW/10/112/raw.las"


def test_presign_parts_for_ingestion_item():
    fake_supabase = _FakeSupabase()
    fake_storage = _FakeUploaderStorage()
    client = _build_client(fake_supabase, fake_storage)

    create_response = client.post(
        "/ingestions",
        json=CreateIngestionRequest(
            dataset_id=10,
            items=[
                IngestionItemCreateInput(
                    dataset_item_id=111,
                    file_name="tile_a.laz",
                    file_size_bytes=2048,
                )
            ],
        ).model_dump(),
    )
    assert create_response.status_code == 200
    created = create_response.json()
    ingestion_id = created["id"]
    ingestion_item_id = created["items"][0]["id"]

    response = client.post(
        f"/ingestions/{ingestion_id}/presign",
        json=PresignPartsRequest(
            ingestion_item_id=ingestion_item_id,
            part_numbers=[1, 2, 3],
        ).model_dump(),
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["ingestion_item_id"] == ingestion_item_id
    assert len(data["parts"]) == 3


def test_complete_ingestion_triggers_existing_jobs_service(monkeypatch):
    fake_supabase = _FakeSupabase()
    fake_storage = _FakeUploaderStorage()
    client = _build_client(fake_supabase, fake_storage)

    create_response = client.post(
        "/ingestions",
        json=CreateIngestionRequest(
            dataset_id=10,
            items=[
                IngestionItemCreateInput(
                    dataset_item_id=111,
                    file_name="tile_a.laz",
                    file_size_bytes=2048,
                )
            ],
        ).model_dump(),
    )
    assert create_response.status_code == 200
    created = create_response.json()
    ingestion_id = created["id"]
    ingestion_item_id = created["items"][0]["id"]

    class _Invocation:
        invocation_id = "inv-123"

    def _fake_create_job(**kwargs):
        assert kwargs["dataset_id"] == "10"
        assert kwargs["workflow_name"] == "EndToEndPipeline"
        return _Invocation()

    monkeypatch.setattr(ingestion_router_module.jobs_service, "create_job", _fake_create_job)

    response = client.post(
        f"/ingestions/{ingestion_id}/complete",
        json=CompleteIngestionRequest(
            items=[
                CompleteIngestionItemRequest(
                    ingestion_item_id=ingestion_item_id,
                    parts=[CompletePart(part_number=1, e_tag="etag-1")],
                )
            ],
            auto_process=True,
            workflow_name="EndToEndPipeline",
        ).model_dump(),
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["status"] == "completed"
    assert payload["workflow_triggered"] is True
    assert payload["workflow_invocation_id"] == "inv-123"
    assert fake_supabase.dataset_items[111]["bucket_path"] == "RAW/10/111/raw.laz"

