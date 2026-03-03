import asyncio
import logging
from datetime import datetime
from threading import Lock
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict

from trees_api.core.config import AppConfig
from trees_api.integrations.galaxy.client import GalaxyClient
from trees_api.integrations.storage.client import StorageClient, UploaderStorageClient
from trees_api.integrations.supabase.client import SupabaseClient

logger = logging.getLogger("uvicorn")


class ClientState(BaseModel):
    """State information for a client connection."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    connected: bool = False
    client: Optional[Any] = None
    last_attempt: Optional[datetime] = None
    last_success: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0


class ConnectionManager:
    """Manage external service clients and reconnection logic."""

    _instance = None
    _lock = Lock()

    def __new__(cls, config: Optional[AppConfig] = None):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config: Optional[AppConfig] = None):
        if not hasattr(self, "_initialized"):
            self.config = config or AppConfig()
            self.galaxy = ClientState()
            self.supabase = ClientState()
            self.storage = ClientState()
            self.uploader_storage = ClientState()
            self._retry_task: Optional[asyncio.Task] = None
            self._initialized = True
        elif config is not None:
            # Allow lifespan startup to inject the already validated config
            # even if the singleton instance was created during module import.
            self.config = config

    def connect_galaxy(self) -> Optional[GalaxyClient]:
        with self._lock:
            if self.galaxy.connected and self.galaxy.client:
                return self.galaxy.client
            try:
                logger.info("Connecting to Galaxy...")
                client = GalaxyClient(self.config.galaxy)
                client.authenticate()
                client.connect()
                self.galaxy.client = client
                self.galaxy.connected = True
                self.galaxy.last_attempt = datetime.now()
                self.galaxy.last_success = datetime.now()
                self.galaxy.error_message = None
                self.galaxy.retry_count = 0
                logger.info("Galaxy connection successful")
                return client
            except Exception as error:
                logger.error("Failed to connect to Galaxy: %s", error)
                self.galaxy.connected = False
                self.galaxy.client = None
                self.galaxy.last_attempt = datetime.now()
                self.galaxy.error_message = str(error)
                self.galaxy.retry_count += 1
                return None

    def connect_supabase(self) -> Optional[SupabaseClient]:
        with self._lock:
            if self.supabase.connected and self.supabase.client:
                return self.supabase.client
            try:
                logger.info("Connecting to Supabase...")
                client = SupabaseClient(self.config.supabase)
                client.connect()
                if (not client.using_service_role) and client.email and client.password:
                    try:
                        client.authenticate_user(client.email, client.password)
                    except Exception as error:
                        error_msg = str(error).lower()
                        if (
                            "authentication failed" in error_msg
                            or "invalid login credentials" in error_msg
                            or "invalid credentials" in error_msg
                        ):
                            try:
                                logger.info("Authentication failed, attempting to register user: %s", client.email)
                                client.register_user(client.email, client.password)
                                logger.info("New user created: %s", client.email)
                            except Exception as reg_error:
                                logger.error("Failed to register user: %s", reg_error)
                                raise Exception(f"Authentication failed and registration failed: {reg_error}")
                        else:
                            raise error
                else:
                    if client.using_service_role:
                        logger.info("Using Supabase with service key (no user authentication)")
                    else:
                        logger.info("Using Supabase without user authentication")
                self.supabase.client = client
                self.supabase.connected = True
                self.supabase.last_attempt = datetime.now()
                self.supabase.last_success = datetime.now()
                self.supabase.error_message = None
                self.supabase.retry_count = 0
                logger.info("Supabase connection successful")
                return client
            except Exception as error:
                logger.error("Failed to connect to Supabase: %s", error)
                self.supabase.connected = False
                self.supabase.client = None
                self.supabase.last_attempt = datetime.now()
                self.supabase.error_message = str(error)
                self.supabase.retry_count += 1
                return None

    def connect_storage(self) -> Optional[StorageClient]:
        with self._lock:
            if self.storage.connected and self.storage.client:
                return self.storage.client
            try:
                logger.info("Connecting to Storage...")
                client = StorageClient(self.config.storage)
                client.connect()
                self.storage.client = client
                self.storage.connected = True
                self.storage.last_attempt = datetime.now()
                self.storage.last_success = datetime.now()
                self.storage.error_message = None
                self.storage.retry_count = 0
                logger.info("Storage connection successful")
                return client
            except Exception as error:
                logger.error("Failed to connect to Storage: %s", error)
                self.storage.connected = False
                self.storage.client = None
                self.storage.last_attempt = datetime.now()
                self.storage.error_message = str(error)
                self.storage.retry_count += 1
                return None

    def connect_uploader_storage(self) -> Optional[UploaderStorageClient]:
        with self._lock:
            if self.uploader_storage.connected and self.uploader_storage.client:
                return self.uploader_storage.client
            try:
                logger.info("Connecting to Storage (uploader credentials)...")
                client = UploaderStorageClient(self.config.storage)
                client.connect()
                self.uploader_storage.client = client
                self.uploader_storage.connected = True
                self.uploader_storage.last_attempt = datetime.now()
                self.uploader_storage.last_success = datetime.now()
                self.uploader_storage.error_message = None
                self.uploader_storage.retry_count = 0
                logger.info("Uploader storage connection successful")
                return client
            except Exception as error:
                logger.error("Failed to connect uploader to Storage: %s", error)
                self.uploader_storage.connected = False
                self.uploader_storage.client = None
                self.uploader_storage.last_attempt = datetime.now()
                self.uploader_storage.error_message = str(error)
                self.uploader_storage.retry_count += 1
                return None

    def get_galaxy_client(self) -> Optional[GalaxyClient]:
        return self.connect_galaxy()

    def get_supabase_client(self) -> Optional[SupabaseClient]:
        return self.connect_supabase()

    def get_storage_client(self) -> Optional[StorageClient]:
        return self.connect_storage()

    def get_uploader_storage_client(self) -> Optional[UploaderStorageClient]:
        return self.connect_uploader_storage()

    def reconnect_all(self):
        if not self.galaxy.connected:
            logger.info("Retrying Galaxy connection...")
            self.connect_galaxy()
        if not self.supabase.connected:
            logger.info("Retrying Supabase connection...")
            self.connect_supabase()
        if not self.storage.connected:
            logger.info("Retrying Storage connection...")
            self.connect_storage()
        if not self.uploader_storage.connected:
            logger.info("Retrying Uploader Storage connection...")
            self.connect_uploader_storage()

    def all_connected(self) -> bool:
        with self._lock:
            return (
                self.galaxy.connected
                and self.supabase.connected
                and self.storage.connected
                and self.uploader_storage.connected
            )

    def get_status(self) -> dict:
        with self._lock:
            return {
                "galaxy": {
                    "connected": self.galaxy.connected,
                    "last_attempt": self.galaxy.last_attempt.isoformat() if self.galaxy.last_attempt else None,
                    "last_success": self.galaxy.last_success.isoformat() if self.galaxy.last_success else None,
                    "error": self.galaxy.error_message,
                    "retry_count": self.galaxy.retry_count,
                },
                "supabase": {
                    "connected": self.supabase.connected,
                    "last_attempt": self.supabase.last_attempt.isoformat() if self.supabase.last_attempt else None,
                    "last_success": self.supabase.last_success.isoformat() if self.supabase.last_success else None,
                    "error": self.supabase.error_message,
                    "retry_count": self.supabase.retry_count,
                },
                "storage": {
                    "connected": self.storage.connected,
                    "last_attempt": self.storage.last_attempt.isoformat() if self.storage.last_attempt else None,
                    "last_success": self.storage.last_success.isoformat() if self.storage.last_success else None,
                    "error": self.storage.error_message,
                    "retry_count": self.storage.retry_count,
                },
                "uploader_storage": {
                    "connected": self.uploader_storage.connected,
                    "last_attempt": self.uploader_storage.last_attempt.isoformat() if self.uploader_storage.last_attempt else None,
                    "last_success": self.uploader_storage.last_success.isoformat() if self.uploader_storage.last_success else None,
                    "error": self.uploader_storage.error_message,
                    "retry_count": self.uploader_storage.retry_count,
                },
            }

    async def start_retry_task(self, interval: int = 60):
        async def retry_loop():
            while True:
                await asyncio.sleep(interval)
                logger.info("Checking client connections...")
                self.reconnect_all()
                if self.all_connected():
                    logger.info("All clients connected successfully")

        self._retry_task = asyncio.create_task(retry_loop())
        return self._retry_task

    async def stop_retry_task(self):
        if self._retry_task and not self._retry_task.done():
            self._retry_task.cancel()
            try:
                await self._retry_task
            except asyncio.CancelledError:
                pass

    def cleanup(self):
        with self._lock:
            if self.supabase.client and self.supabase.connected:
                try:
                    self.supabase.client.sign_out()
                    logger.info("Supabase client signed out")
                except Exception as error:
                    logger.warning("Error during Supabase sign out: %s", error)
            self.galaxy = ClientState()
            self.supabase = ClientState()
            self.storage = ClientState()
            self.uploader_storage = ClientState()


connection_manager = ConnectionManager()
