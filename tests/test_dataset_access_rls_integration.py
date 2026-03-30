import time
from datetime import datetime

import httpx
import pytest

from trees_api.core.config import SupabaseConfig
from trees_api.integrations.supabase.client import SupabaseClient
from tests.supabase_auth_test_utils import (
    authed_supabase_client,
    ensure_user_token,
    password_login_token,
    require_supabase_auth_env,
)


def _require_supabase_auth_env() -> tuple[str, str, str, str]:
    return require_supabase_auth_env(
        skip_prefix="Skipping RLS integration test"
    )


def _query_dataset_rows(supabase_url: str, supabase_key: str, dataset_id: int, token: str | None):
    client = authed_supabase_client(supabase_url, supabase_key, token)
    response = (
        client.table("v_datasets")
        .select("id, visibility, user_id")
        .eq("id", dataset_id)
        .execute()
    )
    return response.data or []


def _resolve_user_identity(supabase_url: str, supabase_key: str, token: str) -> tuple[str, str]:
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


def _query_owner_contacts(supabase_url: str, supabase_key: str, dataset_ids: list[int], token: str):
    client = authed_supabase_client(supabase_url, supabase_key, token)
    return client.rpc(
        "get_dataset_owner_contacts",
        {"dataset_ids": dataset_ids},
    ).execute()


def _update_dataset_description(
    supabase_url: str,
    supabase_key: str,
    dataset_id: int,
    token: str,
    description: str,
):
    client = authed_supabase_client(supabase_url, supabase_key, token)
    return (
        client.table("datasets")
        .update({"description": description})
        .eq("id", dataset_id)
        .eq("archived", False)
        .execute()
    )


@pytest.fixture(scope="module")
def local_supabase_client() -> SupabaseClient:
    _require_supabase_auth_env()
    client = SupabaseClient(SupabaseConfig())
    try:
        client.connect()
        client.authenticate_user(client.email, client.password)
    except Exception as error:
        pytest.skip(f"Skipping RLS integration test: Supabase is not reachable/authenticated: {error}")
    return client


@pytest.fixture(scope="module")
def service_supabase_client() -> SupabaseClient:
    _require_supabase_auth_env()
    client = SupabaseClient(SupabaseConfig())
    try:
        client.connect()
    except Exception as error:
        pytest.skip(f"Skipping RLS integration test: Supabase service-role client unavailable: {error}")
    if not client.using_service_role:
        pytest.skip("Skipping RLS integration test: SUPABASE_SERVICE_KEY is required for fixture setup")
    return client


def test_v_datasets_private_visibility_respects_rls(local_supabase_client: SupabaseClient):
    supabase_url, supabase_key, owner_email, owner_password = _require_supabase_auth_env()
    owner_token = password_login_token(supabase_url, supabase_key, owner_email, owner_password)

    outsider_email = f"rls-outsider-{int(time.time())}@example.test"
    outsider_password = "RlsTestPassw0rd!"
    outsider_token = ensure_user_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=outsider_email,
        password=outsider_password,
    )

    dataset = local_supabase_client.create_dataset(
        bucket_path=f"RAW/rls-private-test/{int(time.time())}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"RLS Private Visibility Test {int(time.time())}",
        file_name="raw.laz",
        visibility="private",
    )

    try:
        owner_rows = _query_dataset_rows(supabase_url, supabase_key, dataset.id, owner_token)
        anon_rows = _query_dataset_rows(supabase_url, supabase_key, dataset.id, token=None)
        outsider_rows = _query_dataset_rows(
            supabase_url, supabase_key, dataset.id, outsider_token
        )

        assert len(owner_rows) == 1, "Owner should read own private dataset from v_datasets"
        assert len(anon_rows) == 0, "Anon role must not read private dataset from v_datasets"
        assert len(outsider_rows) == 0, "Non-owner must not read private dataset from v_datasets"
    finally:
        local_supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()


def test_core_team_member_can_read_private_dataset_and_owner_contacts(
    local_supabase_client: SupabaseClient,
    service_supabase_client: SupabaseClient,
):
    supabase_url, supabase_key, owner_email, owner_password = _require_supabase_auth_env()
    owner_token = password_login_token(supabase_url, supabase_key, owner_email, owner_password)

    core_email = f"rls-core-team-{int(time.time())}@example.test"
    core_password = "RlsCoreTeamPassw0rd!"
    core_token = ensure_user_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=core_email,
        password=core_password,
    )
    core_user_id, core_user_email = _resolve_user_identity(supabase_url, supabase_key, core_token)

    outsider_email = f"rls-core-outsider-{int(time.time())}@example.test"
    outsider_password = "RlsCoreOutsiderPassw0rd!"
    outsider_token = ensure_user_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=outsider_email,
        password=outsider_password,
    )

    dataset = local_supabase_client.create_dataset(
        bucket_path=f"RAW/rls-core-team-test/{int(time.time())}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"RLS Core Team Test {int(time.time())}",
        file_name="raw.laz",
        visibility="private",
    )
    service_supabase_client.client.table("core_team_members").upsert(
        {"user_id": core_user_id, "email": core_user_email}
    ).execute()

    try:
        owner_rows = _query_dataset_rows(supabase_url, supabase_key, dataset.id, owner_token)
        core_rows = _query_dataset_rows(supabase_url, supabase_key, dataset.id, core_token)
        outsider_rows = _query_dataset_rows(supabase_url, supabase_key, dataset.id, outsider_token)

        assert len(owner_rows) == 1
        assert len(core_rows) == 1, "Core team should read private datasets from v_datasets"
        assert len(outsider_rows) == 0

        contacts_response = _query_owner_contacts(
            supabase_url,
            supabase_key,
            [dataset.id],
            core_token,
        )
        contacts = contacts_response.data or []
        assert len(contacts) == 1
        assert contacts[0]["dataset_id"] == dataset.id
        assert contacts[0]["owner_user_id"] == dataset.user_id
        assert contacts[0]["owner_email"] == owner_email
    finally:
        service_supabase_client.client.table("core_team_members").delete().eq(
            "user_id", core_user_id
        ).execute()
        local_supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()


def test_dataset_shared_read_user_can_view_private_without_owner_contacts(
    local_supabase_client: SupabaseClient,
    service_supabase_client: SupabaseClient,
):
    supabase_url, supabase_key, _, _ = _require_supabase_auth_env()

    read_email = f"rls-shared-read-{int(time.time())}@example.test"
    read_password = "RlsSharedReadPassw0rd!"
    read_token = ensure_user_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=read_email,
        password=read_password,
    )
    read_user_id, _ = _resolve_user_identity(supabase_url, supabase_key, read_token)

    dataset = local_supabase_client.create_dataset(
        bucket_path=f"RAW/rls-shared-read-test/{int(time.time())}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"RLS Shared Read Test {int(time.time())}",
        file_name="raw.laz",
        visibility="private",
    )
    service_supabase_client.client.table("dataset_user_access").upsert(
        {
            "dataset_id": dataset.id,
            "grantee_user_id": read_user_id,
            "permission": "read",
            "granted_by_user_id": dataset.user_id,
        }
    ).execute()

    try:
        rows = _query_dataset_rows(supabase_url, supabase_key, dataset.id, read_token)
        assert len(rows) == 1, "Shared-read user should read private dataset"

        contacts_response = _query_owner_contacts(
            supabase_url,
            supabase_key,
            [dataset.id],
            read_token,
        )
        assert (contacts_response.data or []) == []
    finally:
        service_supabase_client.client.table("dataset_user_access").delete().eq(
            "dataset_id", dataset.id
        ).eq("grantee_user_id", read_user_id).execute()
        local_supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()


def test_dataset_shared_edit_user_can_update_private_dataset_metadata(
    local_supabase_client: SupabaseClient,
    service_supabase_client: SupabaseClient,
):
    supabase_url, supabase_key, _, _ = _require_supabase_auth_env()

    edit_email = f"rls-shared-edit-{int(time.time())}@example.test"
    edit_password = "RlsSharedEditPassw0rd!"
    edit_token = ensure_user_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=edit_email,
        password=edit_password,
    )
    edit_user_id, _ = _resolve_user_identity(supabase_url, supabase_key, edit_token)

    dataset = local_supabase_client.create_dataset(
        bucket_path=f"RAW/rls-shared-edit-test/{int(time.time())}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"RLS Shared Edit Test {int(time.time())}",
        file_name="raw.laz",
        visibility="private",
    )
    service_supabase_client.client.table("dataset_user_access").upsert(
        {
            "dataset_id": dataset.id,
            "grantee_user_id": edit_user_id,
            "permission": "edit",
            "granted_by_user_id": dataset.user_id,
        }
    ).execute()

    try:
        update_response = _update_dataset_description(
            supabase_url=supabase_url,
            supabase_key=supabase_key,
            dataset_id=dataset.id,
            token=edit_token,
            description="Updated by shared-edit user",
        )
        assert len(update_response.data or []) == 1, "Shared-edit user should update dataset row"

        stored = (
            service_supabase_client.client.table("datasets")
            .select("id, description")
            .eq("id", dataset.id)
            .limit(1)
            .execute()
        )
        rows = stored.data or []
        assert len(rows) == 1
        assert rows[0]["description"] == "Updated by shared-edit user"
    finally:
        service_supabase_client.client.table("dataset_user_access").delete().eq(
            "dataset_id", dataset.id
        ).eq("grantee_user_id", edit_user_id).execute()
        local_supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()
