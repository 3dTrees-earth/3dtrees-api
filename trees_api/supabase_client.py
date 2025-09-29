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

from trees_api.models import Dataset, WorkflowInvocation

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
    invocations_table: str = Field(default="galaxy_workflow_invocations", description="Supabase workflow invocations table name")

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

    def get_datasets(self, user_id: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[Dataset]:
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        def _execute_query():
            query = self.client.table(self.datasets_table).select("*")
            if user_id is not None:
                query = query.eq("user_id", user_id)
            
            datasets = []
            response = query.order("created_at", desc=True).limit(limit).offset(offset).execute()
            for dataset in response.data:
                datasets.append(Dataset.model_validate(dataset))
            return datasets
        
        try:
            return _execute_query()
        except Exception as e:
            if "JWT expired" in str(e):
                # Try to re-authenticate and retry once
                logger.warning("JWT expired, attempting to re-authenticate...")
                try:
                    self.authenticate_user(self.email, self.password)
                    return _execute_query()
                except Exception as retry_e:
                    raise RuntimeError(f"Failed to get datasets after re-authentication: {retry_e}") from retry_e
            else:
                raise RuntimeError(f"Failed to get datasets: {e}") from e

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

    def create_workflow_invocation(self, workflow_uuid: str, dataset_id: int, workflow_name: str) -> WorkflowInvocation:
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        response = self.client.table(self.invocations_table).insert({
            "dataset_id": dataset_id,
            "invocation_id": workflow_uuid,
            "workflow_name": workflow_name,
            "status": "new",  # Galaxy state for newly created invocations
            "started_at": datetime.now().isoformat(),
            "inputs": [],  # Initialize as empty list
            "steps": [],   # Initialize as empty list
            "outputs": {}, # Initialize as empty dict
            "output_collections": {}, # Initialize as empty dict
            "jobs": [],    # Initialize as empty list
            "messages": [], # Initialize as empty list
            "parameters": {}, # Initialize as empty dict
        }).execute()

        return WorkflowInvocation.model_validate(response.data[0])
    
    def get_workflow_invocations(self, status: Optional[str] = None, limit: int = 100, offset: int = 0, results_synced: Optional[bool] = None) -> List[WorkflowInvocation]:
        """
        Get workflow invocations from Supabase.
        
        Args:
            status: Optional status filter
            limit: Maximum number of invocations to return
            offset: Number of invocations to skip
            results_synced: Optional filter for results_synced field
            
        Returns:
            List of WorkflowInvocation objects
            
        Raises:
            RuntimeError: If not connected to Supabase
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        try:
            query = self.client.table(self.invocations_table).select("*")
            
            if status is not None:
                query = query.eq("status", status)
            
            if results_synced is not None:
                query = query.eq("results_synced", results_synced)
            
            response = query.order("created_at", desc=True).limit(limit).offset(offset).execute()
            
            invocations = []
            for invocation_data in response.data:
                invocations.append(WorkflowInvocation.model_validate(invocation_data))
            
            logger.info(f"Retrieved {len(invocations)} workflow invocations from Supabase")
            return invocations
            
        except Exception as e:
            raise RuntimeError(f"Failed to get workflow invocations: {e}") from e
    
    def get_workflow_invocations_by_dataset_id(self, dataset_id: int, limit: int = 100, offset: int = 0) -> List[WorkflowInvocation]:
        """
        Get workflow invocations for a specific dataset_id.
        
        Args:
            dataset_id: The dataset ID to filter by (as string)
            limit: Maximum number of invocations to return
            offset: Number of invocations to skip
            
        Returns:
            List of WorkflowInvocation objects
            
        Raises:
            RuntimeError: If not connected to Supabase
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        try:
            response = self.client.table(self.invocations_table).select("*").eq("dataset_id", dataset_id).order("created_at", desc=True).limit(limit).offset(offset).execute()
            
            invocations = []
            for invocation_data in response.data:
                invocations.append(WorkflowInvocation.model_validate(invocation_data))
            
            logger.info(f"Retrieved {len(invocations)} workflow invocations for dataset {dataset_id}")
            return invocations
            
        except Exception as e:
            raise RuntimeError(f"Failed to get workflow invocations for dataset {dataset_id}: {e}") from e
    
    def get_workflow_invocation_by_id(self, invocation_id: str) -> Optional[WorkflowInvocation]:
        """
        Get a specific workflow invocation by invocation_id.
        
        Args:
            invocation_id: The invocation ID to look for
            
        Returns:
            WorkflowInvocation object if found, None otherwise
            
        Raises:
            RuntimeError: If not connected to Supabase
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        try:
            response = self.client.table(self.invocations_table).select("*").eq("invocation_id", invocation_id).execute()
            
            if response.data:
                return WorkflowInvocation.model_validate(response.data[0])
            return None
            
        except Exception as e:
            raise RuntimeError(f"Failed to get workflow invocation {invocation_id}: {e}") from e
    
    def update_workflow_invocation(self, invocation_id: str, **updates) -> WorkflowInvocation:
        """
        Update a workflow invocation in Supabase.
        
        Args:
            invocation_id: The invocation ID to update
            **updates: Fields to update (status, steps, inputs, outputs, jobs, messages, finished_at, etc.)
            
        Returns:
            Updated WorkflowInvocation object
            
        Raises:
            RuntimeError: If not connected to Supabase
            LookupError: If invocation not found
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        try:
            # Convert datetime objects to ISO strings if present
            update_data = {}
            for key, value in updates.items():
                if hasattr(value, 'isoformat'):  # datetime object
                    update_data[key] = value.isoformat()
                else:
                    update_data[key] = value
            
            response = self.client.table(self.invocations_table).update(update_data).eq("invocation_id", invocation_id).execute()
            
            if not response.data:
                raise LookupError(f"Workflow invocation {invocation_id} not found")
            
            logger.info(f"Updated workflow invocation {invocation_id} with: {list(updates.keys())}")
            return WorkflowInvocation.model_validate(response.data[0])
            
        except LookupError:
            raise
        except Exception as e:
            raise RuntimeError(f"Failed to update workflow invocation {invocation_id}: {e}") from e
    
    def get_workflow_invocations_by_status(self, status: str) -> List[WorkflowInvocation]:
        """
        Get all workflow invocations with a specific status.
        
        Args:
            status: The status to filter by
            
        Returns:
            List of WorkflowInvocation objects with the specified status
            
        Raises:
            RuntimeError: If not connected to Supabase
        """
        return self.get_workflow_invocations(status=status)
    
    def get_unfinished_workflow_invocations(self) -> List[WorkflowInvocation]:
        """
        Get all workflow invocations that are not finished (not successful or errored).
        
        Returns:
            List of unfinished WorkflowInvocation objects
            
        Raises:
            RuntimeError: If not connected to Supabase
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        try:
            # Get invocations that are not finished (not in Galaxy's terminal states)
            # Galaxy terminal states: 'ok', 'success', 'error', 'failed', 'cancelled', 'deleted', 'discarded', 'warning'
            terminal_states = ["ok", "success", "error", "failed", "cancelled", "deleted", "discarded", "warning"]
            response = self.client.table(self.invocations_table).select("*").not_.in_("status", terminal_states).execute()
            
            invocations = []
            for invocation_data in response.data:
                invocations.append(WorkflowInvocation.model_validate(invocation_data))
            
            logger.info(f"Retrieved {len(invocations)} unfinished workflow invocations from Supabase")
            return invocations
            
        except Exception as e:
            raise RuntimeError(f"Failed to get unfinished workflow invocations: {e}") from e