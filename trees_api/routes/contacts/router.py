"""Contact sync endpoints for CRM integration."""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from trees_api.integrations.contacts import BrevoCRMService, ContactData
from trees_api.integrations.contacts.brevo import BrevoContactsConfig
from trees_api.core.config import BrevoConfig

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/contacts", tags=["contacts"])


class SubscribeRequest(BaseModel):
    email: str
    source: str | None = None


class ContactSyncResponse(BaseModel):
    success: bool


def _get_crm_service() -> BrevoCRMService:
    config = BrevoConfig()
    if not config.api_key:
        raise HTTPException(status_code=503, detail="CRM service not configured")
    return BrevoCRMService(
        BrevoContactsConfig(
            api_key=config.api_key,
            subscriber_list_id=config.subscriber_list_id,
        )
    )


@router.post("/subscribe", response_model=ContactSyncResponse)
def subscribe(request: SubscribeRequest) -> ContactSyncResponse:
    """Add a contact to the subscriber mailing list."""
    crm = _get_crm_service()
    try:
        crm.add_subscriber(ContactData(email=request.email, source=request.source))
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to sync subscriber %s", request.email)
        raise HTTPException(status_code=502, detail="CRM sync failed")
    return ContactSyncResponse(success=True)
