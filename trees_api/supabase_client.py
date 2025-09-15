from typing import Optional, Dict, Any, List
import os
import logging
from pathlib import Path
from uuid import uuid4
from datetime import datetime

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions

from trees_api.models import Dataset

logger = logging.getLogger("uvicorn")


class SupabaseClient(BaseSettings):
    """Supabase client for 3DTrees API with Pydantic settings configuration."""
    
    url: str = Field(default="", description="Supabase project URL")
    key: str = Field(default="", description="Supabase anon/public key")
    service_key: Optional[str] = Field(default=None, description="Supabase service role key (for admin operations)")
    
    # Supabase user credentials (separate from Galaxy credentials)
    email: Optional[str] = Field(default="processor@3dtrees.earth", description="Supabase user email")
    password: Optional[str] = Field(default=None, description="Supabase user password")
    
    # optional settings to overwrite table names
    datasets_table: str = Field(default="datasets", description="Supabase datasets table name")

    # Client instance
    client: Optional[Client] = Field(default=None, init=False)
    
    model_config = SettingsConfigDict(
        case_sensitive=False,
        cli_parse_args=True,
        cli_ignore_unknown_args=True,
        env_file=".env",
        env_prefix="SUPABASE_",
    )
    
    def connect(self) -> bool:
        if not self.url or not self.key:
            raise ValueError("Supabase URL and key are required. Set SUPABASE_URL and SUPABASE_KEY in .env file.")
            
        try:
            logger.debug(f"Connecting to Supabase at {self.url}...")
            
            # Create client options
            options = ClientOptions(
                schema="public",
                headers={
                    "X-Client-Info": "3dtrees-api/0.1.0"
                }
            )
            
            # Create Supabase client
            self.client = create_client(
                supabase_url=self.url,
                supabase_key=self.key,
                options=options
            )
            
            # Test connection by getting user info (if authenticated)
            try:
                user = self.client.auth.get_user()
                if user:
                    logger.info(f"Connected to Supabase as user: {user.user.email}")
                else:
                    logger.info("Connected to Supabase (anonymous)")
            except Exception:
                logger.info("Connected to Supabase (anonymous)")
                
            return True
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Supabase: {e}")
    
    def authenticate_user(self, email: str, password: str) -> Dict[str, Any]:
        """
        Authenticate a user with email and password.
        
        Args:
            email: User email
            password: User password
            
        Returns:
            User session data if successful
            
        Raises:
            RuntimeError: If not connected to Supabase
            ValueError: If authentication fails
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
            
        try:
            logger.debug(f"Authenticating user: {email}")
            response = self.client.auth.sign_in_with_password({
                "email": email,
                "password": password
            })
            
            if response.user:
                logger.info(f"Successfully authenticated user: {email}")
                return {
                    "user": response.user,
                    "session": response.session
                }
            else:
                raise ValueError("Authentication failed - no user returned")
                
        except ValueError:
            # Re-raise ValueError as-is (our own validation error)
            raise
        except Exception as e:
            # Wrap unexpected exceptions with context
            raise RuntimeError(f"Authentication failed: {e}") from e
    
    def register_user(self, email: str, password: str) -> Dict[str, Any]:
        """
        Register a new user with email and password.
        
        Args:
            email: User email
            password: User password
            
        Returns:
            User session data if successful
            
        Raises:
            RuntimeError: If not connected to Supabase
            ValueError: If registration fails
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        response = self.client.auth.sign_up({
            "email": email,
            "password": password
        })
        
        if response.user:
            logger.info(f"Successfully registered user: {email}")
            return {
                "user": response.user,
                "session": response.session
            }
        else:
            raise ValueError("Registration failed - no user returned")
                
    def get_current_user(self) -> Optional[Dict[str, Any]]:
        """
        Get the currently authenticated user.
        
        Returns:
            User data if authenticated, None if not authenticated
            
        Raises:
            RuntimeError: If not connected to Supabase
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
            
        try:
            user = self.client.auth.get_user()
            if user and user.user:
                return {"user": user.user, "session": getattr(user, 'session', None)}
            return None
        except Exception as e:
            logger.error(f"Error getting current user: {e}")
            return None
    
    def sign_out(self) -> bool:
        """
        Sign out the current user.
        
        Returns:
            True if successful
            
        Raises:
            RuntimeError: If not connected to Supabase
            ValueError: If sign out fails
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
            
        try:
            self.client.auth.sign_out()
            logger.info("User signed out successfully")
            return True
        except Exception as e:
            # Wrap unexpected exceptions with context
            raise RuntimeError(f"Sign out failed: {e}") from e

    def get_dataset(self, dataset_id: Optional[int] = None, uuid: Optional[str] = None) -> Dataset:
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        if dataset_id is None and uuid is None:
            raise ValueError("Either dataset_id or uuid must be provided")
        
        query = self.client.table(self.datasets_table).select("*")
        if dataset_id is not None:
            query = query.eq("id", dataset_id)
        else:
            query = query.eq("uuid", uuid)
        
        response = query.execute()
        return Dataset.model_validate(response.data[0])

    def create_dataset(self, bucket_path: str,acquisition_date: datetime, title: str = None, file_name: str = None, visibility: str = None) -> Dataset:
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        user_id = self.get_current_user()["user"].id

        response = self.client.table(self.datasets_table).insert({
            "uuid": str(uuid4()),
            "user_id": user_id,
            "bucket_path": bucket_path,
            "acquisition_date": acquisition_date.isoformat(),
            "title": title,
            "file_name": file_name,
            "visibility": visibility
        }).execute()

        return Dataset.model_validate(response.data[0])