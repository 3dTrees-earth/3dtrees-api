"""CRM service abstraction for contact management."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class ContactData:
    """Contact information to sync with the CRM."""

    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class CRMService(ABC):
    """Abstract interface for CRM contact operations.

    Implementations can target Brevo, HubSpot, Mailchimp, etc.
    """

    @abstractmethod
    def add_subscriber(self, contact: ContactData) -> None:
        """Add a contact to the subscriber/mailing list."""

    @abstractmethod
    def add_user(self, contact: ContactData) -> None:
        """Add a contact to the registered-user list."""
