from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
import time
from datetime import datetime
from types import SimpleNamespace

from trees_api.core.config import SupabaseConfig
from trees_api.integrations.supabase.client import SupabaseClient
from trees_api.routes.downloads.router import get_supabase_client as get_downloads_supabase_client
from trees_api.routes.jobs.router import (
    get_galaxy_client,
    get_storage_client,
    get_supabase_client,
    router,
)
from tests.supabase_auth_test_utils import (
    ensure_user_token,
    password_login_token,
    require_supabase_auth_env,
)


class FakeGalaxyClient:
    def __init__(self) -> None:
        self.config = SimpleNamespace(
            file_source_products="products-storage",
            file_source_visualization="visualization-storage",
            file_source_scheme="gxfiles",
            default_object_store_id=None,
            default_intermediate_object_store_id=None,
            default_outputs_object_store_id=None,
        )

    def create_history(self, name: str):
        return SimpleNamespace(id=f"history-{int(time.time() * 1000)}")

    def delete_history(self, history_id: str, purge: bool = True) -> bool:
        return True

    def get_workflow_structure(self, workflow_name: str):
        return {"steps": {}}

    def invoke_workflow_with_collection(self, **kwargs):
        return {"invocation_id": f"invocation-{int(time.time() * 1000)}"}


@pytest.fixture(scope="module")
def local_supabase_client() -> SupabaseClient:
    require_supabase_auth_env(skip_prefix="Skipping jobs authz integration test")
    client = SupabaseClient(SupabaseConfig())
    try:
        client.connect()
        client.authenticate_user(client.email, client.password)
    except Exception as error:
        pytest.skip(
            f"Skipping jobs authz integration test: Supabase is not reachable/authenticated: {error}"
        )
    return client


@pytest.fixture(scope="module")
def service_supabase_client() -> SupabaseClient:
    require_supabase_auth_env(skip_prefix="Skipping jobs authz integration test")
    client = SupabaseClient(SupabaseConfig())
    try:
        client.connect()
    except Exception as error:
        pytest.skip(
            f"Skipping jobs authz integration test: Supabase service-role client unavailable: {error}"
        )
    if not client.using_service_role:
        pytest.skip(
            "Skipping jobs authz integration test: SUPABASE_SERVICE_KEY is required for fixture setup"
        )
    return client


@pytest.fixture(scope="module")
def auth_tokens() -> tuple[str, str]:
    supabase_url, supabase_key, owner_email, owner_password = require_supabase_auth_env(
        skip_prefix="Skipping jobs authz integration test"
    )

    owner_token = password_login_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=owner_email,
        password=owner_password,
    )

    outsider_token = ensure_user_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=f"jobs-outsider-{int(time.time())}@example.test",
        password="JobsAuthzPassw0rd!",
    )
    return owner_token, outsider_token


@pytest.fixture
def jobs_api_client(local_supabase_client: SupabaseClient) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_supabase_client] = lambda: local_supabase_client
    app.dependency_overrides[get_downloads_supabase_client] = lambda: local_supabase_client
    app.dependency_overrides[get_galaxy_client] = lambda: FakeGalaxyClient()
    app.dependency_overrides[get_storage_client] = lambda: object()
    return TestClient(app)


def _resolve_user_identity(supabase_url: str, supabase_key: str, token: str) -> tuple[str, str]:
    import httpx

    response = httpx.get(
        f"{supabase_url}/auth/v1/user",
        headers={
            "apikey": supabase_key,
            "Authorization": f"Bearer {token}",
        },
        timeout=20.0,
    )
    response.raise_for_status()
    payload = response.json()
    return payload["id"], payload["email"]


def _create_dataset_for_user(
    *,
    supabase_url: str,
    supabase_key: str,
    email: str,
    password: str,
    title_prefix: str,
):
    client = SupabaseClient(SupabaseConfig(url=supabase_url, key=supabase_key, email=email, password=password))
    client.connect()
    client.authenticate_user(email, password)
    return client, client.create_dataset(
        bucket_path=f"RAW/jobs-authz/{int(time.time() * 1000)}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"{title_prefix} {int(time.time() * 1000)}",
        file_name="raw.laz",
        visibility="private",
    )


def test_create_job_denies_non_owner(jobs_api_client: TestClient, local_supabase_client: SupabaseClient, auth_tokens: tuple[str, str]):
    _, outsider_token = auth_tokens
    dataset = local_supabase_client.create_dataset(
        bucket_path=f"RAW/jobs-authz/{int(time.time())}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"Jobs Authz {int(time.time())}",
        file_name="raw.laz",
        visibility="private",
    )

    try:
        response = jobs_api_client.post(
            "/jobs",
            params={"dataset_id": str(dataset.id), "workflow_name": "EndToEndPipeline"},
            headers={"Authorization": f"Bearer {outsider_token}"},
            json={},
        )
        assert response.status_code == 403
        assert "access denied" in response.json()["detail"].lower()
    finally:
        local_supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()


def test_list_jobs_returns_empty_for_foreign_dataset(jobs_api_client: TestClient, local_supabase_client: SupabaseClient, auth_tokens: tuple[str, str]):
    _, outsider_token = auth_tokens
    dataset = local_supabase_client.create_dataset(
        bucket_path=f"RAW/jobs-authz/{int(time.time())}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"Jobs List Authz {int(time.time())}",
        file_name="raw.laz",
        visibility="private",
    )

    try:
        response = jobs_api_client.get(
            "/jobs",
            params={"dataset_id": dataset.id},
            headers={"Authorization": f"Bearer {outsider_token}"},
        )
        assert response.status_code == 200
        assert response.json() == []
    finally:
        local_supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()


def test_list_jobs_allows_owner_on_own_dataset(
    jobs_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
):
    owner_token, _ = auth_tokens
    dataset = local_supabase_client.create_dataset(
        bucket_path=f"RAW/jobs-authz/{int(time.time())}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"Jobs Owner List Authz {int(time.time())}",
        file_name="raw.laz",
        visibility="private",
    )

    try:
        response = jobs_api_client.get(
            "/jobs",
            params={"dataset_id": dataset.id},
            headers={"Authorization": f"Bearer {owner_token}"},
        )
        assert response.status_code == 200, response.text
        assert isinstance(response.json(), list)
    finally:
        local_supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()


def test_jobs_requires_authorization_header(jobs_api_client: TestClient):
    response = jobs_api_client.post(
        "/jobs",
        params={"dataset_id": "1", "workflow_name": "EndToEndPipeline"},
        json={},
    )
    assert response.status_code == 401


def test_processor_can_create_job_for_foreign_dataset(
    jobs_api_client: TestClient,
    service_supabase_client: SupabaseClient,
):
    supabase_url, supabase_key, owner_email, owner_password = require_supabase_auth_env(
        skip_prefix="Skipping jobs authz integration test"
    )
    if owner_email != "processor@3dtrees.earth":
        pytest.skip("Processor rerun authz test requires SUPABASE_EMAIL=processor@3dtrees.earth")

    processor_token = password_login_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=owner_email,
        password=owner_password,
    )
    foreign_email = f"jobs-foreign-owner-{int(time.time())}@example.test"
    foreign_password = "JobsForeignOwnerPassw0rd!"
    ensure_user_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=foreign_email,
        password=foreign_password,
    )
    foreign_client, dataset = _create_dataset_for_user(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=foreign_email,
        password=foreign_password,
        title_prefix="Jobs processor foreign dataset",
    )

    try:
        response = jobs_api_client.post(
            "/jobs",
            params={"dataset_id": str(dataset.id), "workflow_name": "EndToEndPipeline-GalaxyEU"},
            headers={"Authorization": f"Bearer {processor_token}"},
            json={},
        )
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["dataset_id"] == dataset.id
    finally:
        service_supabase_client.client.table("galaxy_workflow_invocations").delete().eq(
            "dataset_id", dataset.id
        ).execute()
        service_supabase_client.client.table("galaxy_histories").delete().eq(
            "dataset_id", dataset.id
        ).execute()
        foreign_client.client.table("datasets").delete().eq("id", dataset.id).execute()


def test_shared_editor_can_create_job_for_private_dataset(
    jobs_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    service_supabase_client: SupabaseClient,
):
    supabase_url, supabase_key, _, _ = require_supabase_auth_env(
        skip_prefix="Skipping jobs authz integration test"
    )
    shared_email = f"jobs-shared-editor-{int(time.time())}@example.test"
    shared_password = "JobsSharedEditorPassw0rd!"
    shared_token = ensure_user_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=shared_email,
        password=shared_password,
    )
    shared_user_id, _ = _resolve_user_identity(supabase_url, supabase_key, shared_token)

    dataset = local_supabase_client.create_dataset(
        bucket_path=f"RAW/jobs-authz/{int(time.time() * 1000)}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"Jobs shared editor {int(time.time() * 1000)}",
        file_name="raw.laz",
        visibility="private",
    )
    service_supabase_client.client.table("dataset_user_access").upsert(
        {
            "dataset_id": dataset.id,
            "grantee_user_id": shared_user_id,
            "permission": "edit",
            "granted_by_user_id": dataset.user_id,
        }
    ).execute()

    try:
        response = jobs_api_client.post(
            "/jobs",
            params={"dataset_id": str(dataset.id), "workflow_name": "EndToEndPipeline-GalaxyEU"},
            headers={"Authorization": f"Bearer {shared_token}"},
            json={},
        )
        assert response.status_code == 200, response.text
    finally:
        service_supabase_client.client.table("dataset_user_access").delete().eq(
            "dataset_id", dataset.id
        ).eq("grantee_user_id", shared_user_id).execute()
        service_supabase_client.client.table("galaxy_workflow_invocations").delete().eq(
            "dataset_id", dataset.id
        ).execute()
        service_supabase_client.client.table("galaxy_histories").delete().eq(
            "dataset_id", dataset.id
        ).execute()
        local_supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()


def test_processor_can_list_jobs_for_foreign_dataset(
    jobs_api_client: TestClient,
    service_supabase_client: SupabaseClient,
):
    supabase_url, supabase_key, owner_email, owner_password = require_supabase_auth_env(
        skip_prefix="Skipping jobs authz integration test"
    )
    if owner_email != "processor@3dtrees.earth":
        pytest.skip("Processor rerun authz test requires SUPABASE_EMAIL=processor@3dtrees.earth")

    processor_token = password_login_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=owner_email,
        password=owner_password,
    )
    foreign_email = f"jobs-list-foreign-{int(time.time())}@example.test"
    foreign_password = "JobsListForeignPassw0rd!"
    ensure_user_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=foreign_email,
        password=foreign_password,
    )
    foreign_client, dataset = _create_dataset_for_user(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=foreign_email,
        password=foreign_password,
        title_prefix="Jobs processor list foreign dataset",
    )

    try:
        service_supabase_client.client.table("galaxy_workflow_invocations").insert(
            {
                "invocation_id": f"processor-list-{int(time.time() * 1000)}",
                "dataset_id": dataset.id,
                "workflow_name": "EndToEndPipeline-GalaxyEU",
                "status": "new",
                "inputs": {},
                "steps": [],
                "outputs": {},
                "output_collections": {},
                "jobs": [],
                "messages": [],
                "parameters": {},
            }
        ).execute()

        response = jobs_api_client.get(
            "/jobs",
            params={"dataset_id": dataset.id},
            headers={"Authorization": f"Bearer {processor_token}"},
        )
        assert response.status_code == 200, response.text
        assert any(row["dataset_id"] == dataset.id for row in response.json())
    finally:
        service_supabase_client.client.table("galaxy_workflow_invocations").delete().eq(
            "dataset_id", dataset.id
        ).execute()
        foreign_client.client.table("datasets").delete().eq("id", dataset.id).execute()
