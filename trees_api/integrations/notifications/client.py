from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import httpx


@dataclass(frozen=True)
class BrevoEmailConfig:
    api_url: str
    api_key: str
    sender_email: str
    sender_name: str
    reply_to_email: Optional[str] = None
    reply_to_name: Optional[str] = None


@dataclass(frozen=True)
class EmailRecipient:
    email: str
    name: Optional[str] = None


@dataclass(frozen=True)
class EmailMessage:
    subject: str
    to: List[EmailRecipient]
    html_content: str
    text_content: str


def send_email_via_brevo(*, config: BrevoEmailConfig, message: EmailMessage) -> None:
    """Send an email via Brevo transactional email API."""
    if not config.api_key:
        raise RuntimeError("BREVO_API_KEY is not configured")
    if not message.to:
        raise RuntimeError("Email recipient list is empty")

    payload = {
        "sender": {"email": config.sender_email, "name": config.sender_name},
        "to": [
            {"email": recipient.email, **({"name": recipient.name} if recipient.name else {})}
            for recipient in message.to
        ],
        "subject": message.subject,
        "htmlContent": message.html_content,
        "textContent": message.text_content,
    }
    if config.reply_to_email:
        payload["replyTo"] = {
            "email": config.reply_to_email,
            **({"name": config.reply_to_name} if config.reply_to_name else {}),
        }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": config.api_key,
    }

    with httpx.Client(timeout=30.0) as client:
        response = client.post(config.api_url, headers=headers, json=payload)
        response.raise_for_status()


__all__ = [
    "BrevoEmailConfig",
    "EmailRecipient",
    "EmailMessage",
    "send_email_via_brevo",
]
