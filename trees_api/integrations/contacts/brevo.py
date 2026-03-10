"""Brevo CRM implementation for contact management."""

import logging
from dataclasses import dataclass

import httpx

from trees_api.integrations.contacts.service import ContactData, CRMService

logger = logging.getLogger(__name__)

BREVO_CONTACTS_API_URL = "https://api.brevo.com/v3/contacts"


@dataclass(frozen=True)
class BrevoContactsConfig:
    """Configuration for Brevo contact sync."""

    api_key: str
    subscriber_list_id: int


class BrevoCRMService(CRMService):
    """Brevo implementation of the CRM contact service."""

    def __init__(self, config: BrevoContactsConfig) -> None:
        self._config = config

    def add_subscriber(self, contact: ContactData) -> None:
        self._upsert_contact(contact, list_ids=[self._config.subscriber_list_id])

    def _upsert_contact(
        self, contact: ContactData, list_ids: list[int]
    ) -> None:
        attributes: dict[str, str] = {}
        if contact.source:
            attributes["SOURCE"] = contact.source

        payload: dict = {
            "email": contact.email,
            "listIds": list_ids,
            "updateEnabled": True,
        }
        if attributes:
            payload["attributes"] = attributes

        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "api-key": self._config.api_key,
        }

        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                BREVO_CONTACTS_API_URL, headers=headers, json=payload
            )
            # 201 = created, 204 = updated (already exists with updateEnabled)
            if response.status_code not in (201, 204):
                logger.error(
                    "Brevo contact sync failed: %s %s",
                    response.status_code,
                    response.text,
                )
                raise httpx.HTTPStatusError(
                    f"Unexpected status {response.status_code}",
                    request=response.request,
                    response=response,
                )

        logger.info(
            "Synced contact %s to Brevo lists %s",
            contact.email,
            list_ids,
        )
