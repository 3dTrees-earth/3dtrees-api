import os

import httpx
import pytest
from supabase import Client, create_client


def require_supabase_auth_env(skip_prefix: str) -> tuple[str, str, str, str]:
    required = ["SUPABASE_URL", "SUPABASE_KEY", "SUPABASE_EMAIL", "SUPABASE_PASSWORD"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        pytest.skip(f"{skip_prefix}: missing env vars: {', '.join(missing)}")

    return (
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_KEY"],
        os.environ["SUPABASE_EMAIL"],
        os.environ["SUPABASE_PASSWORD"],
    )


def password_login_token(
    supabase_url: str,
    supabase_key: str,
    email: str,
    password: str,
) -> str:
    response = httpx.post(
        f"{supabase_url}/auth/v1/token?grant_type=password",
        headers={
            "apikey": supabase_key,
            "Content-Type": "application/json",
        },
        json={
            "email": email,
            "password": password,
        },
        timeout=20.0,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("No access_token returned by Supabase auth endpoint")
    return token


def ensure_user_token(
    supabase_url: str,
    supabase_key: str,
    email: str,
    password: str,
) -> str:
    try:
        return password_login_token(supabase_url, supabase_key, email, password)
    except Exception:
        signup_response = httpx.post(
            f"{supabase_url}/auth/v1/signup",
            headers={
                "apikey": supabase_key,
                "Content-Type": "application/json",
            },
            json={
                "email": email,
                "password": password,
            },
            timeout=20.0,
        )
        if signup_response.status_code >= 500:
            signup_response.raise_for_status()
        return password_login_token(supabase_url, supabase_key, email, password)


def authed_supabase_client(supabase_url: str, supabase_key: str, token: str | None) -> Client:
    client = create_client(supabase_url, supabase_key)
    if token:
        client.postgrest.auth(token)
    return client
