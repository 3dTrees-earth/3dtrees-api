from __future__ import annotations

from datetime import datetime, timezone
import html

from trees_api.integrations.notifications.client import (
    BrevoEmailConfig,
    EmailMessage,
    EmailRecipient,
    send_email_via_brevo,
)



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
    account_downloads_url = "https://3dtrees.earth/account?tab=downloads"
    safe_dataset_title = html.escape(dataset_title, quote=True)
    safe_archive_filename = html.escape(archive_filename, quote=True)
    safe_signed_url = html.escape(signed_url, quote=True)
    safe_dataset_url = html.escape(dataset_url, quote=True)
    safe_account_downloads_url = html.escape(account_downloads_url, quote=True)
    safe_expires_text = html.escape(expires_text, quote=True)
    return (
        '<div style="font-family: Arial, sans-serif; line-height: 1.5; color: #111827;">'
        '<h2 style="margin-bottom: 8px;">Your 3Dtrees download is ready</h2>'
        '<p style="margin-top: 0;">'
        f"Dataset <strong>{dataset_id}</strong> ({safe_dataset_title}) has been packaged for download."
        "</p>"
        '<div style="margin: 20px 0;">'
        f'<a href="{safe_signed_url}" '
        'style="background:#2563eb;color:#ffffff;text-decoration:none;padding:10px 14px;'
        'border-radius:6px;display:inline-block;">'
        "Download archive"
        "</a>"
        "</div>"
        '<p style="margin: 0;">'
        f"<strong>Archive:</strong> {safe_archive_filename}<br>"
        f"<strong>Expires:</strong> {safe_expires_text}"
        "</p>"
        '<p style="margin-top: 12px;">'
        f'Account downloads: <a href="{safe_account_downloads_url}">{safe_account_downloads_url}</a>'
        "</p>"
        '<p style="margin-top: 8px;">'
        "If the button above does not open directly, copy the download URL from this email and open it in your browser."
        "</p>"
        '<p style="margin-top: 8px;">'
        f'Dataset page: <a href="{safe_dataset_url}">{safe_dataset_url}</a>'
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
    account_downloads_url = "https://3dtrees.earth/account?tab=downloads"
    return (
        "Your 3Dtrees download is ready.\n\n"
        f"Dataset: {dataset_id} ({dataset_title})\n"
        f"Archive: {archive_filename}\n"
        f"Expires: {expires_text}\n\n"
        f"Download: {signed_url}\n"
        f"Account downloads: {account_downloads_url}\n"
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
    message = EmailMessage(
        subject=_build_download_email_subject(archive_filename),
        to=[EmailRecipient(email=to_email)],
        html_content=_build_download_email_html(
            archive_filename=archive_filename,
            signed_url=signed_url,
            dataset_id=dataset_id,
            dataset_title=dataset_title,
            expires_at=signed_url_expires_at,
        ),
        text_content=_build_download_email_text(
            archive_filename=archive_filename,
            signed_url=signed_url,
            dataset_id=dataset_id,
            dataset_title=dataset_title,
            expires_at=signed_url_expires_at,
        ),
    )
    send_email_via_brevo(config=config, message=message)


__all__ = ["BrevoEmailConfig", "send_download_ready_email"]

