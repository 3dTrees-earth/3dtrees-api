import time
from datetime import datetime

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

        assert len(owner_rows) == 1, "Owner should be able to read own private dataset from v_datasets"
        assert len(anon_rows) == 0, "Anon role must not read private dataset from v_datasets"
        assert len(outsider_rows) == 0, "Non-owner must not read private dataset from v_datasets"
    finally:
        local_supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()
