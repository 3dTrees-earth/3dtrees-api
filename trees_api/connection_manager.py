import logging
import asyncio
from datetime import datetime
from typing import Optional, Any
from threading import Lock

from pydantic import BaseModel, Field

from trees_api.config import AppConfig
from trees_api.galaxy_client import GalaxyClient
from trees_api.supabase_client import SupabaseClient
from trees_api.storage_client import StorageClient, UploaderStorageClient

logger = logging.getLogger("uvicorn")

class ClientState(BaseModel):
    """State information for a client connection."""
    connected: bool = False
    client: Optional[Any] = None
    last_attempt: Optional[datetime] = None
    last_success: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    
    class Config:
        arbitrary_types_allowed = True  # Allow non-Pydantic types like client instances

class ConnectionManager:
    """
    Manages connections to all external services (Galaxy, Supabase, Storage).
    Holds client instances and handles reconnection logic.
    """
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls, config: Optional[AppConfig] = None):
        """Singleton pattern to ensure only one manager exists."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config: Optional[AppConfig] = None):
        """
        Initialize the connection manager.
        
        Args:
            config: AppConfig instance with all service configurations.
                    If None, creates default config (for backward compatibility).
        """
        if not hasattr(self, '_initialized'):
            self.config = config or AppConfig()
            self.galaxy = ClientState()
            self.supabase = ClientState()
            self.storage = ClientState()  # Processor storage (read raw, write products)
            self.uploader_storage = ClientState()  # Uploader storage (write raw)
            self._retry_task: Optional[asyncio.Task] = None
            self._initialized = True
    
    def connect_galaxy(self) -> Optional[GalaxyClient]:
        """
        Connect to Galaxy. Returns cached client if already connected.
        """
        with self._lock:
            # Return cached client if already connected
            if self.galaxy.connected and self.galaxy.client:
                return self.galaxy.client
            
            # Try to connect
            try:
                logger.info("Connecting to Galaxy...")
                client = GalaxyClient(self.config.galaxy)
                client.authenticate()
                client.connect()
                
                # Update state
                self.galaxy.client = client
                self.galaxy.connected = True
                self.galaxy.last_attempt = datetime.now()
                self.galaxy.last_success = datetime.now()
                self.galaxy.error_message = None
                self.galaxy.retry_count = 0
                
                logger.info("Galaxy connection successful")
                return client
                
            except Exception as e:
                logger.error(f"Failed to connect to Galaxy: {e}")
                self.galaxy.connected = False
                self.galaxy.client = None
                self.galaxy.last_attempt = datetime.now()
                self.galaxy.error_message = str(e)
                self.galaxy.retry_count += 1
                return None
    
    def connect_supabase(self) -> Optional[SupabaseClient]:
        """
        Connect to Supabase. Returns cached client if already connected.
        """
        with self._lock:
            # Return cached client if already connected
            if self.supabase.connected and self.supabase.client:
                return self.supabase.client
            
            # Try to connect
            try:
                logger.info("Connecting to Supabase...")
                client = SupabaseClient(self.config.supabase)
                client.connect()
                
                # Authenticate only when running without service-role key.
                # Backend orchestration should prefer service-role auth to avoid
                # expiring user JWT sessions.
                if (not client.using_service_role) and client.email and client.password:
                    try:
                        client.authenticate_user(client.email, client.password)
                    except Exception as e:
                        # Only attempt registration if it's an authentication failure
                        error_msg = str(e).lower()
                        if "authentication failed" in error_msg or "invalid login credentials" in error_msg or "invalid credentials" in error_msg:
                            try:
                                logger.info(f"Authentication failed, attempting to register user: {client.email}")
                                client.register_user(client.email, client.password)
                                logger.info(f"New user created: {client.email}")
                            except Exception as reg_error:
                                logger.error(f"Failed to register user: {reg_error}")
                                raise Exception(f"Authentication failed and registration failed: {reg_error}")
                        else:
                            raise e
                else:
                    if client.using_service_role:
                        logger.info("Using Supabase with service key (no user authentication)")
                    else:
                        logger.info("Using Supabase without user authentication")
                
                # Update state
                self.supabase.client = client
                self.supabase.connected = True
                self.supabase.last_attempt = datetime.now()
                self.supabase.last_success = datetime.now()
                self.supabase.error_message = None
                self.supabase.retry_count = 0
                
                logger.info("Supabase connection successful")
                return client
                
            except Exception as e:
                logger.error(f"Failed to connect to Supabase: {e}")
                self.supabase.connected = False
                self.supabase.client = None
                self.supabase.last_attempt = datetime.now()
                self.supabase.error_message = str(e)
                self.supabase.retry_count += 1
                return None
    
    def connect_storage(self) -> Optional[StorageClient]:
        """
        Connect to Storage. Returns cached client if already connected.
        """
        with self._lock:
            # Return cached client if already connected
            if self.storage.connected and self.storage.client:
                return self.storage.client
            
            # Try to connect
            try:
                logger.info("Connecting to Storage...")
                client = StorageClient(self.config.storage)
                client.connect()
                
                # Update state
                self.storage.client = client
                self.storage.connected = True
                self.storage.last_attempt = datetime.now()
                self.storage.last_success = datetime.now()
                self.storage.error_message = None
                self.storage.retry_count = 0
                
                logger.info("Storage connection successful")
                return client
                
            except Exception as e:
                logger.error(f"Failed to connect to Storage: {e}")
                self.storage.connected = False
                self.storage.client = None
                self.storage.last_attempt = datetime.now()
                self.storage.error_message = str(e)
                self.storage.retry_count += 1
                return None
    
    def connect_uploader_storage(self) -> Optional[UploaderStorageClient]:
        """
        Connect to Storage with uploader credentials. Returns cached client if already connected.
        Used for generating presigned URLs for frontend uploads to raw bucket.
        """
        with self._lock:
            # Return cached client if already connected
            if self.uploader_storage.connected and self.uploader_storage.client:
                return self.uploader_storage.client
            
            # Try to connect
            try:
                logger.info("Connecting to Storage (uploader credentials)...")
                client = UploaderStorageClient(self.config.storage)
                client.connect()
                
                # Update state
                self.uploader_storage.client = client
                self.uploader_storage.connected = True
                self.uploader_storage.last_attempt = datetime.now()
                self.uploader_storage.last_success = datetime.now()
                self.uploader_storage.error_message = None
                self.uploader_storage.retry_count = 0
                
                logger.info("Uploader storage connection successful")
                return client
                
            except Exception as e:
                logger.error(f"Failed to connect uploader to Storage: {e}")
                self.uploader_storage.connected = False
                self.uploader_storage.client = None
                self.uploader_storage.last_attempt = datetime.now()
                self.uploader_storage.error_message = str(e)
                self.uploader_storage.retry_count += 1
                return None
    
    def get_galaxy_client(self) -> Optional[GalaxyClient]:
        """Get Galaxy client (from cache or create new connection)."""
        return self.connect_galaxy()
    
    def get_supabase_client(self) -> Optional[SupabaseClient]:
        """Get Supabase client (from cache or create new connection)."""
        return self.connect_supabase()
    
    def get_storage_client(self) -> Optional[StorageClient]:
        """Get Storage client with processor credentials (from cache or create new connection)."""
        return self.connect_storage()
    
    def get_uploader_storage_client(self) -> Optional[UploaderStorageClient]:
        """Get Storage client with uploader credentials (from cache or create new connection)."""
        return self.connect_uploader_storage()
    
    def reconnect_all(self):
        """Attempt to reconnect all disconnected clients."""
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
        """Check if all clients are connected."""
        with self._lock:
            return (self.galaxy.connected and self.supabase.connected and 
                    self.storage.connected and self.uploader_storage.connected)
    
    def get_status(self) -> dict:
        """Get status of all client connections."""
        with self._lock:
            return {
                "galaxy": {
                    "connected": self.galaxy.connected,
                    "last_attempt": self.galaxy.last_attempt.isoformat() if self.galaxy.last_attempt else None,
                    "last_success": self.galaxy.last_success.isoformat() if self.galaxy.last_success else None,
                    "error": self.galaxy.error_message,
                    "retry_count": self.galaxy.retry_count
                },
                "supabase": {
                    "connected": self.supabase.connected,
                    "last_attempt": self.supabase.last_attempt.isoformat() if self.supabase.last_attempt else None,
                    "last_success": self.supabase.last_success.isoformat() if self.supabase.last_success else None,
                    "error": self.supabase.error_message,
                    "retry_count": self.supabase.retry_count
                },
                "storage": {
                    "connected": self.storage.connected,
                    "last_attempt": self.storage.last_attempt.isoformat() if self.storage.last_attempt else None,
                    "last_success": self.storage.last_success.isoformat() if self.storage.last_success else None,
                    "error": self.storage.error_message,
                    "retry_count": self.storage.retry_count
                },
                "uploader_storage": {
                    "connected": self.uploader_storage.connected,
                    "last_attempt": self.uploader_storage.last_attempt.isoformat() if self.uploader_storage.last_attempt else None,
                    "last_success": self.uploader_storage.last_success.isoformat() if self.uploader_storage.last_success else None,
                    "error": self.uploader_storage.error_message,
                    "retry_count": self.uploader_storage.retry_count
                }
            }
    
    async def start_retry_task(self, interval: int = 60):
        """Start background task to retry failed connections."""
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
        """Stop the background retry task."""
        if self._retry_task and not self._retry_task.done():
            self._retry_task.cancel()
            try:
                await self._retry_task
            except asyncio.CancelledError:
                pass
    
    def cleanup(self):
        """Cleanup all client connections."""
        with self._lock:
            # Sign out from Supabase
            if self.supabase.client and self.supabase.connected:
                try:
                    self.supabase.client.sign_out()
                    logger.info("Supabase client signed out")
                except Exception as e:
                    logger.warning(f"Error during Supabase sign out: {e}")
            
            # Reset all states
            self.galaxy = ClientState()
            self.supabase = ClientState()
            self.storage = ClientState()
            self.uploader_storage = ClientState()

# Global instance
connection_manager = ConnectionManager()
