from typing import Optional, Dict, Any, List
import os
import logging
from pathlib import Path
from uuid import uuid4
from datetime import datetime

from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions

from trees_api.models import Dataset, WorkflowInvocation
from trees_api.config import SupabaseConfig

logger = logging.getLogger("uvicorn")


class SupabaseClient:
    """Supabase client for 3DTrees API."""
    
    def __init__(self, config: SupabaseConfig):
        """
        Initialize Supabase client with configuration.
        
        Args:
            config: SupabaseConfig instance with connection details
        """
        self.url = config.url
        self.key = config.key
        self.service_key = config.service_key
        self.email = config.email
        self.password = config.password
        self.datasets_table = config.datasets_table
        self.invocations_table = config.invocations_table
        
        self.client: Optional[Client] = None
    
    def connect(self) -> bool:
        if not self.url or not self.key:
            raise ValueError("Supabase URL and key are required. Set SUPABASE_URL and SUPABASE_KEY in .env file.")
            
        try:
            logger.debug(f"Connecting to Supabase at {self.url}...")
            
            # Create Supabase client (simplified - newer supabase-py doesn't require ClientOptions)
            self.client = create_client(
                supabase_url=self.url,
                supabase_key=self.key
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

    def get_dataset_item(self, dataset_item_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a dataset_item by ID.
        
        Args:
            dataset_item_id: ID of the dataset_item to retrieve
            
        Returns:
            Dictionary with dataset_item data including id, bucket_path, file_name, dataset_id
            
        Raises:
            RuntimeError: If not connected to Supabase
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        try:
            response = self.client.table("dataset_items").select("*").eq("id", dataset_item_id).execute()
            if response.data:
                return response.data[0]
            return None
        except Exception as e:
            logger.error(f"Failed to get dataset_item {dataset_item_id}: {e}")
            return None

    def get_dataset(self, dataset_id: Optional[int] = None, uuid: Optional[str] = None) -> Dataset:
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        if dataset_id is None and uuid is None:
            raise ValueError("Either dataset_id or uuid must be provided")
        
        # Get dataset with first dataset_item (for single-file datasets)
        query = self.client.table("datasets").select("*, dataset_items:dataset_items(*)")
        if dataset_id is not None:
            query = query.eq("id", dataset_id)
        else:
            query = query.eq("uuid", uuid)
        
        response = query.execute()
        dataset_data = response.data[0]
        
        # Extract bucket_path and file_name from first dataset_item
        dataset_items = dataset_data.get("dataset_items", [])
        bucket_path = dataset_items[0].get("bucket_path", "") if dataset_items else ""
        file_name = dataset_items[0].get("file_name") if dataset_items else None
        
        # Create Dataset object with bucket_path and file_name from dataset_item
        dataset_dict = {
            **dataset_data,
            "bucket_path": bucket_path,
            "file_name": file_name,
        }
        # Remove dataset_items from the dict since it's not part of Dataset model
        dataset_dict.pop("dataset_items", None)
        
        return Dataset.model_validate(dataset_dict)

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

    def create_dataset(self, bucket_path: str, acquisition_date: datetime, title: str = None, file_name: str = None, visibility: str = None) -> Dataset:
        """Legacy method: Creates dataset and dataset_item. Note: bucket_path and file_name are now in dataset_items."""
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        user_id = self.get_current_user()["user"].id

        # Create parent dataset record
        dataset_response = self.client.table(self.datasets_table).insert({
            "uuid": str(uuid4()),
            "user_id": user_id,
            "acquisition_date": acquisition_date.isoformat(),
            "title": title or "Untitled Dataset",
            "visibility": visibility or "private"
        }).execute()
        
        dataset_id = dataset_response.data[0]["id"]
        
        # Create dataset_item record
        self.client.table("dataset_items").insert({
            "dataset_id": dataset_id,
            "bucket_path": bucket_path,
            "file_name": file_name
        }).execute()

        # Return Dataset object with bucket_path and file_name
        dataset_dict = {
            **dataset_response.data[0],
            "bucket_path": bucket_path,
            "file_name": file_name,
        }
        
        return Dataset.model_validate(dataset_dict)

    def create_workflow_invocation(self, workflow_uuid: str, dataset_id: int, workflow_name: str, history_fk: Optional[int] = None) -> WorkflowInvocation:
        """
        Create a workflow invocation record.
        
        Args:
            workflow_uuid: Galaxy invocation ID
            dataset_id: ID of the dataset (parent of dataset_items)
            workflow_name: Name of the workflow
            history_fk: Optional ID of the galaxy_histories record to link
            
        Returns:
            WorkflowInvocation object
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        invocation_data = {
            "dataset_id": dataset_id,
            "invocation_id": workflow_uuid,
            "workflow_name": workflow_name,
            "status": "new",  # Galaxy state for newly created invocations
            "started_at": datetime.now().isoformat(),
            "inputs": {},  # Galaxy returns inputs as dict with step indices as keys
            "steps": [],   # Initialize as empty list
            "outputs": {}, # Initialize as empty dict
            "output_collections": {}, # Initialize as empty dict
            "jobs": [],    # Initialize as empty list
            "messages": [], # Initialize as empty list
            "parameters": {}, # Initialize as empty dict
        }
        
        # Link to galaxy_history if provided
        if history_fk is not None:
            invocation_data["history_fk"] = history_fk
        
        response = self.client.table(self.invocations_table).insert(invocation_data).execute()

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
            dataset_id: The dataset ID to filter by
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
            # Query invocations directly by dataset_id
            response = (
                self.client.table(self.invocations_table)
                .select("*")
                .eq("dataset_id", dataset_id)
                .order("created_at", desc=True)
                .limit(limit)
                .offset(offset)
                .execute()
            )
            
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
    
    def get_finished_unsynced_workflows(self) -> List[WorkflowInvocation]:
        """
        Get all workflow invocations that have finished_at set but results_synced is False.
        This catches workflows that were completed via job state detection.
        
        Returns:
            List of WorkflowInvocation objects that are finished but not synced
            
        Raises:
            RuntimeError: If not connected to Supabase
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        try:
            response = self.client.table(self.invocations_table).select("*").not_.is_("finished_at", "null").eq("results_synced", False).execute()
            
            invocations = []
            for invocation_data in response.data:
                invocations.append(WorkflowInvocation.model_validate(invocation_data))
            
            logger.info(f"Retrieved {len(invocations)} finished but unsynced workflow invocations from Supabase")
            return invocations
            
        except Exception as e:
            raise RuntimeError(f"Failed to get finished unsynced workflow invocations: {e}") from e
    
    def upsert_product_metadata(
        self,
        table: str,
        dataset_item_id: int,
        data: Dict[str, Any]
    ) -> None:
        """
        Upsert metadata into a product table (standard, overviews, segmentations, tilesets).
        
        This method checks if a row exists for the given dataset_item_id and either updates
        the existing row or inserts a new one.
        
        Args:
            table: Name of the product table to update
            dataset_item_id: ID of the dataset item
            data: Dictionary of field values to upsert
            
        Raises:
            RuntimeError: If not connected to Supabase or operation fails
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        try:
            # Check if row exists
            existing = (
                self.client.table(table)
                .select("id")
                .eq("dataset_item_id", dataset_item_id)
                .execute()
            )
            
            if existing.data:
                # Update existing row
                logger.debug(f"Updating existing {table} record for dataset_item_id {dataset_item_id}")
                self.client.table(table).update(data).eq("dataset_item_id", dataset_item_id).execute()
            else:
                # Insert new row
                logger.debug(f"Inserting new {table} record for dataset_item_id {dataset_item_id}")
                data["dataset_item_id"] = dataset_item_id
                self.client.table(table).insert(data).execute()
            
            logger.info(f"Successfully upserted {table} metadata for dataset_item_id {dataset_item_id}")
        
        except Exception as e:
            logger.error(f"Error upserting {table} metadata for dataset_item_id {dataset_item_id}: {e}")
            raise RuntimeError(f"Failed to upsert {table} metadata: {e}") from e

    # =========================================================================
    # Galaxy Histories CRUD
    # =========================================================================
    
    def get_or_create_galaxy_history(
        self,
        dataset_id: int,
        history_id: str,
        history_name: str,
        s3_base_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get existing galaxy_history for a dataset, or create a new one.
        
        Each dataset has exactly one galaxy_history (1:1 relationship).
        If a history already exists for the dataset, returns it.
        Otherwise, creates a new one with the provided Galaxy history_id.
        
        Args:
            dataset_id: The dataset ID (FK to datasets table)
            history_id: Galaxy's history ID (assigned by Galaxy)
            history_name: Human-readable name for the history
            s3_base_path: Base S3 path for exports (e.g., "{history_id}/")
            
        Returns:
            Dictionary with galaxy_history record
            
        Raises:
            RuntimeError: If not connected to Supabase or operation fails
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        try:
            # Check if history already exists for this dataset
            existing = (
                self.client.table("galaxy_histories")
                .select("*")
                .eq("dataset_id", dataset_id)
                .execute()
            )
            
            if existing.data:
                logger.debug(f"Found existing galaxy_history for dataset {dataset_id}")
                return existing.data[0]
            
            # Create new galaxy_history
            new_history = {
                "dataset_id": dataset_id,
                "history_id": history_id,
                "history_name": history_name,
                "s3_base_path": s3_base_path or f"{history_id}/",
                "outputs": {},
            }
            
            # Insert and return the record
            response = self.client.table("galaxy_histories").insert(new_history).execute()
            
            if not response.data:
                raise RuntimeError("Failed to create galaxy_history - no data returned")
            
            logger.info(f"Created galaxy_history for dataset {dataset_id}: {history_name}")
            return response.data[0]
            
        except Exception as e:
            logger.error(f"Error in get_or_create_galaxy_history for dataset {dataset_id}: {e}")
            raise RuntimeError(f"Failed to get/create galaxy_history: {e}") from e
    
    def get_galaxy_history_by_dataset(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        """
        Get galaxy_history for a dataset.
        
        Args:
            dataset_id: The dataset ID
            
        Returns:
            Dictionary with galaxy_history record, or None if not found
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        try:
            response = (
                self.client.table("galaxy_histories")
                .select("*")
                .eq("dataset_id", dataset_id)
                .execute()
            )
            
            return response.data[0] if response.data else None
            
        except Exception as e:
            logger.error(f"Error getting galaxy_history for dataset {dataset_id}: {e}")
            raise RuntimeError(f"Failed to get galaxy_history: {e}") from e
    
    def get_galaxy_history_by_history_id(self, history_id: str) -> Optional[Dict[str, Any]]:
        """
        Get galaxy_history by Galaxy's history ID.
        
        Args:
            history_id: Galaxy's history ID
            
        Returns:
            Dictionary with galaxy_history record, or None if not found
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        try:
            response = (
                self.client.table("galaxy_histories")
                .select("*")
                .eq("history_id", history_id)
                .execute()
            )
            
            return response.data[0] if response.data else None
            
        except Exception as e:
            logger.error(f"Error getting galaxy_history by history_id {history_id}: {e}")
            raise RuntimeError(f"Failed to get galaxy_history: {e}") from e
    
    def update_galaxy_history_outputs(
        self,
        history_id: str,
        outputs: Dict[str, Any]
    ) -> None:
        """
        Update the outputs JSONB field for a galaxy_history.
        
        This is used by the status pooler to accumulate product outputs
        as workflow steps complete.
        
        Args:
            history_id: Galaxy's history ID
            outputs: Dictionary of outputs to store (replaces existing)
            
        Raises:
            RuntimeError: If not connected or update fails
        """
        if not self.client:
            raise RuntimeError("Not connected to Supabase. Call connect() first.")
        
        try:
            self.client.table("galaxy_histories").update({
                "outputs": outputs,
                "updated_at": datetime.now().isoformat()
            }).eq("history_id", history_id).execute()
            
            logger.debug(f"Updated outputs for galaxy_history {history_id}")
            
        except Exception as e:
            logger.error(f"Error updating galaxy_history outputs for {history_id}: {e}")
            raise RuntimeError(f"Failed to update galaxy_history outputs: {e}") from e