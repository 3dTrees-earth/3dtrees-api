"""CRM service abstraction for contact management."""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class ContactData:
    """Contact information to sync with the CRM."""

    email: str
    source: str | None = None


class CRMService(ABC):
    """Abstract interface for CRM contact operations.

    Implementations can target Brevo, HubSpot, Mailchimp, etc.
    """

    @abstractmethod
    def add_subscriber(self, contact: ContactData) -> None:
        """Add a contact to the subscriber/mailing list."""
