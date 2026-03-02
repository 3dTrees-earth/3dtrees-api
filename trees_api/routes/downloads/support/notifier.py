from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import httpx


@dataclass(frozen=True)
class BrevoEmailConfig:
    api_url: str
    api_key: str
    sender_email: str
    sender_name: str


def _build_download_email_subject(archive_filename: str) -> str:
    return f"Your 3Dtrees download is ready: {archive_filename}"


def _build_download_email_html(
    *,
    archive_filename: str,
    signed_url: str,
    dataset_id: int,
    dataset_title: str,
    expires_at: datetime,
) -> str:
    expires_text = expires_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    dataset_url = f"https://3dtrees.earth/datasets/{dataset_id}"
    return (
        '<div style="font-family: Arial, sans-serif; line-height: 1.5; color: #111827;">'
        '<h2 style="margin-bottom: 8px;">Your 3Dtrees download is ready</h2>'
        '<p style="margin-top: 0;">'
        f"Dataset <strong>{dataset_id}</strong> ({dataset_title}) has been packaged for download."
        "</p>"
        '<div style="margin: 20px 0;">'
        f'<a href="{signed_url}" '
        'style="background:#2563eb;color:#ffffff;text-decoration:none;padding:10px 14px;'
        'border-radius:6px;display:inline-block;">'
        "Download archive"
        "</a>"
        "</div>"
        '<p style="margin: 0;">'
        f"<strong>Archive:</strong> {archive_filename}<br>"
        f"<strong>Expires:</strong> {expires_text}"
        "</p>"
        '<p style="margin-top: 12px;">'
        f'Dataset page: <a href="{dataset_url}">{dataset_url}</a>'
        "</p>"
        '<hr style="border:none;border-top:1px solid #e5e7eb;margin:20px 0;">'
        '<p style="font-size: 12px; color: #6b7280; margin: 0;">'
        "This is an automated message from 3Dtrees."
        "</p>"
        "</div>"
    )


def _build_download_email_text(
    *,
    archive_filename: str,
    signed_url: str,
    dataset_id: int,
    dataset_title: str,
    expires_at: datetime,
) -> str:
    expires_text = expires_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    dataset_url = f"https://3dtrees.earth/datasets/{dataset_id}"
    return (
        "Your 3Dtrees download is ready.\n\n"
        f"Dataset: {dataset_id} ({dataset_title})\n"
        f"Archive: {archive_filename}\n"
        f"Expires: {expires_text}\n\n"
        f"Download: {signed_url}\n"
        f"Dataset page: {dataset_url}\n"
    )


def send_download_ready_email(
    *,
    config: BrevoEmailConfig,
    to_email: str,
    archive_filename: str,
    signed_url: str,
    dataset_id: int,
    dataset_title: str,
    signed_url_expires_at: datetime,
) -> None:
    if not config.api_key:
        raise RuntimeError("BREVO_API_KEY is not configured")

    payload = {
        "sender": {"email": config.sender_email, "name": config.sender_name},
        "to": [{"email": to_email}],
        "subject": _build_download_email_subject(archive_filename),
        "htmlContent": _build_download_email_html(
            archive_filename=archive_filename,
            signed_url=signed_url,
            dataset_id=dataset_id,
            dataset_title=dataset_title,
            expires_at=signed_url_expires_at,
        ),
        "textContent": _build_download_email_text(
            archive_filename=archive_filename,
            signed_url=signed_url,
            dataset_id=dataset_id,
            dataset_title=dataset_title,
            expires_at=signed_url_expires_at,
        ),
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": config.api_key,
    }

    with httpx.Client(timeout=30.0) as client:
        response = client.post(config.api_url, headers=headers, json=payload)
        response.raise_for_status()


__all__ = ["BrevoEmailConfig", "send_download_ready_email"]

