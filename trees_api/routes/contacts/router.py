"""Contact sync endpoints for CRM integration."""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from trees_api.integrations.contacts import BrevoCRMService, ContactData, CRMService
from trees_api.integrations.contacts.brevo import BrevoContactsConfig
from trees_api.core.config import AppConfig

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/contacts", tags=["contacts"])


class SubscribeRequest(BaseModel):
    email: str
    source: Optional[str] = None


class SyncUserRequest(BaseModel):
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class ContactSyncResponse(BaseModel):
    success: bool


def _get_crm_service() -> CRMService:
    config = AppConfig().brevo
    if not config.is_configured():
        raise HTTPException(status_code=503, detail="CRM service not configured")
    return BrevoCRMService(
        BrevoContactsConfig(
            api_key=config.api_key,
            subscriber_list_id=config.subscriber_list_id,
            user_list_id=config.user_list_id,
        )
    )


@router.post("/subscribe", response_model=ContactSyncResponse)
def subscribe(request: SubscribeRequest) -> ContactSyncResponse:
    """Add a contact to the subscriber mailing list."""
    crm = _get_crm_service()
    try:
        crm.add_subscriber(ContactData(email=request.email, source=request.source))
    except Exception:
        logger.exception("Failed to sync subscriber %s", request.email)
        raise HTTPException(status_code=502, detail="CRM sync failed")
    return ContactSyncResponse(success=True)


@router.post("/sync-user", response_model=ContactSyncResponse)
def sync_user(request: SyncUserRequest) -> ContactSyncResponse:
    """Add or update a registered user in the CRM."""
    crm = _get_crm_service()
    try:
        crm.add_user(
            ContactData(
                email=request.email,
                first_name=request.first_name,
                last_name=request.last_name,
            )
        )
    except Exception:
        logger.exception("Failed to sync user %s", request.email)
        raise HTTPException(status_code=502, detail="CRM sync failed")
    return ContactSyncResponse(success=True)
