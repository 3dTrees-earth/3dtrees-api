import json
import os
import time
import base64
import logging
import requests
from pathlib import Path
from typing import Optional, Dict, Any, List

from bioblend.galaxy.objects import GalaxyInstance as GalaxyObjectsInstance
from bioblend.galaxy.objects.wrappers import Workflow

from trees_api.config import GalaxyConfig

logger = logging.getLogger("uvicorn")


def count_tool_steps_from_workflow(workflow_data: Dict[str, Any]) -> int:
    """
    Count the number of tool steps (non-input steps) in a workflow definition.
    
    Args:
        workflow_data: Workflow definition dict (from .ga file or Galaxy API)
        
    Returns:
        Number of tool steps that should create jobs
    """
    if not workflow_data or 'steps' not in workflow_data:
        return 0
    
    tool_step_count = 0
    for step in workflow_data['steps'].values():
        # Count steps that have a tool_id (these create jobs)
        # Skip input steps (type='data_input' or tool_id is None)
        if step.get('tool_id') and step.get('type') != 'data_input':
            tool_step_count += 1
    
    return tool_step_count


class GalaxyClient:
    """
    Galaxy client for 3DTrees API.
    
    Handles authentication, workflow management, and dataset operations.
    """
    
    def __init__(self, config: GalaxyConfig):
        """
        Initialize Galaxy client with configuration.
        
        Args:
            config: GalaxyConfig instance with connection details
        """
        self.config = config  # Store full config for file source access
        self.url = config.url
        self.api_key = config.api_key
        self.email = config.email
        self.password = config.password
        self.admin_key = config.admin_key
        self.workflows_path = config.resolved_workflows_path()
        
        self.workflow_registry: Dict[str, str] = {}
        self.gi: Optional[GalaxyObjectsInstance] = None
    
    def get_raw_file_source_uri(self, bucket_path: str) -> str:
        """
        Build a file source URI for the raw storage bucket using config.
        
        Returns a file source URI using configured scheme and file source ID.
        Galaxy will fetch the file directly from S3 using the file source credentials.
        
        Args:
            bucket_path: Path within the bucket (e.g., "RAW/167/94/raw.laz")
            
        Returns:
            File source URI for Galaxy (e.g., "gxuserfiles://uuid/RAW/167/94/raw.laz")
        """
        return self.build_file_source_uri(
            self.config.file_source_raw,
            bucket_path,
            scheme=self.config.file_source_scheme
        )
    
    def get_products_file_source_uri(self, bucket_path: str) -> str:
        """
        Build a file source URI for the products storage bucket using config.
        
        Args:
            bucket_path: Path within the bucket (e.g., "standard/123/456/")
            
        Returns:
            Complete file source URI using configured scheme and file source ID
        """
        return self.build_file_source_uri(
            self.config.file_source_products,
            bucket_path,
            scheme=self.config.file_source_scheme
        )
    
    def setup_user_with_bootstrap(self, email: Optional[str] = None, password: Optional[str] = None) -> bool:
        """
        Create a user and API key using the bootstrap admin API key.
        This method should be called when Galaxy is running with bootstrap_admin_api_key configured.
        
            
        Returns:
            True if user and API key were created successfully
            
        Raises:
            RuntimeError: If user creation or API key generation fails
        """
        if not self.admin_key:
            raise ValueError("Bootstrap admin API key is not set")
        
        if email is None:
            email = self.email
        if password is None:
            password = self.password
            
        if not email or not password:
            raise ValueError("User email and password are required")
            
        logger.info(f"Setting up user {email} using bootstrap admin API key...")
        
        headers = {
            "x-api-key": self.admin_key,
            "Content-Type": "application/json"
        }
        
        # Create user
        user_data = {
            "email": email,
            "password": password,
            "username": email.split('@')[0]
        }
        
        try:
            response = requests.post(
                f"{self.url}/api/users",
                json=user_data,
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                user_info = response.json()
                logger.info(f"User created successfully: {user_info['id']}")
            elif response.status_code == 400 and "already exists" in response.text.lower():
                logger.info(f"User {email} already exists")
                # Get user info
                response = requests.get(
                    f"{self.url}/api/users?email={email}",
                    headers=headers,
                    timeout=10
                )
                if response.status_code == 200:
                    users = response.json()
                    if users:
                        user_info = users[0]
                    else:
                        raise RuntimeError(f"User {email} exists but cannot be retrieved")
                else:
                    raise RuntimeError(f"Failed to get user info: {response.status_code}")
            else:
                raise RuntimeError(f"Failed to create user: {response.status_code} - {response.text}")
                
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Error creating user: {e}") from e
        
        # Create API key for the user
        api_key_data = {
            "name": "3dtrees_api_key"
        }
        
        try:
            response = requests.post(
                f"{self.url}/api/users/{user_info['id']}/api_keys",
                json=api_key_data,
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                api_key_info = response.json()
                self.api_key = api_key_info['key']
                logger.info(f"API key created successfully for user {user_info['id']}")
                
                # Store the API key in environment variable
                os.environ["GALAXY_API_KEY"] = self.api_key
                logger.info("API key stored in environment variable")
                return True
            else:
                raise RuntimeError(f"Failed to create API key: {response.status_code} - {response.text}")
                
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Error creating API key: {e}") from e
       
    def authenticate(self, email: Optional[str] = None, password: Optional[str] = None) -> bool:
        """
        Authenticate with Galaxy and get API key.
        
        Args:
            email: User email (defaults to GALAXY_EMAIL from env)
            password: User password (defaults to GALAXY_PASSWORD from env)
            
        Returns:
            True if authentication successful
            
        Raises:
            ValueError: If email or password is missing
            RuntimeError: If authentication fails
        """
        if self.api_key:
            logger.debug("Using existing Galaxy API key")
            return True
            
        logger.info("No Galaxy API key found, attempting authentication...")
        if email is None:
            email = self.email
        if password is None:
            password = self.password
        
        if not email or not password:
            raise ValueError("Galaxy email and password are required for authentication. You can set GALAXY_EMAIL and GALAXY_PASSWORD in the .env file or pass them as arguments.")

        self.api_key = self._get_or_create_api_key(email, password)
        
        if not self.api_key:
            raise RuntimeError("Failed to authenticate and get Galaxy API key")
            
        # Store the API key in environment variable for future use
        os.environ["GALAXY_API_KEY"] = self.api_key
        logger.debug("Galaxy API key stored in environment variable")
        return True
    
    def _get_or_create_api_key(self, email: str, password: str) -> str:
        """
        Get API key using baseauth, or create user if it doesn't exist.
        Based on Galaxy API documentation: https://galaxyproject.org/develop/api/
        
        Returns:
            API key if successful
            
        Raises:
            RuntimeError: If authentication fails
        """
        # First try to get API key using baseauth
        auth_string = base64.b64encode(f"{email}:{password}".encode()).decode()
        headers = {"Authorization": f"Basic {auth_string}"}
        
        try:
            response = requests.get(f"{self.url}/api/authenticate/baseauth", headers=headers)
            if response.status_code == 200:
                api_key = response.json().get('api_key')
                if api_key:
                    logger.info(f"Successfully authenticated and got API key for {email}")
                    return api_key
        except Exception as e:
            logger.debug(f"Baseauth failed: {e}")
        
        # If baseauth fails, user might not exist or credentials are wrong
        # Note: User creation is not automated - users must be created manually in Galaxy
        # If user creation API returns 501 (not implemented), user likely exists but needs manual creation
        logger.debug(f"Baseauth failed. User {email} may need to be created manually in Galaxy.")
        logger.debug("Skipping automated user creation - users must be created manually.")
        
        # Try one more time in case user was just created manually
        try:
            response = requests.get(f"{self.url}/api/authenticate/baseauth", headers=headers)
            if response.status_code == 200:
                api_key = response.json().get('api_key')
                if api_key:
                    logger.info(f"Successfully authenticated and got API key for {email}")
                    return api_key
        except Exception as e:
            logger.debug(f"Final authentication attempt failed: {e}")
        
        raise RuntimeError(f"Failed to authenticate user {email}. Please ensure the user exists in Galaxy and credentials are correct.")
    
    def connect(self) -> bool:
        """
        Connect to Galaxy instance.
        
        Returns:
            True if connection successful
            
        Raises:
            RuntimeError: If no API key available
            RuntimeError: If connection fails
        """
        if not self.api_key:
            raise RuntimeError("No Galaxy API key available. Call authenticate() first.")
            
        try:
            logger.debug(f"Connecting to Galaxy at {self.url}...")
            self.gi = GalaxyObjectsInstance(self.url, self.api_key)
            
            # Test connection
            version_info = self.gi.gi.make_get_request(f"{self.url}/api/version").json()
            logger.info(f"Connected to Galaxy version: {version_info.get('version_major', 'Unknown')}")
            
            # Load workflow registry after successful connection
            self._load_workflow_registry()
            
            return True
            
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Galaxy: {e}") from e
    
    def _find_workflow_by_uuid(self, workflow_uuid: str) -> Workflow:
        """
        Find a workflow by UUID
        
        Args:
            workflow_uuid: UUID of the workflow to find
            
        Returns:
            Workflow object if found
            
        Raises:
            RuntimeError: If not connected to Galaxy
            LookupError: If workflow is not found
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")

        logger.debug(f"Searching for workflow with UUID: {workflow_uuid}")
        # List workflows from Galaxy
        workflows = self.gi.workflows.list()
        
        for workflow in workflows:
            if hasattr(workflow, 'latest_workflow_uuid') and workflow.latest_workflow_uuid == workflow_uuid:
                logger.info(f"Found workflow: {workflow.name} (ID: {workflow.id})")
                return workflow
                
        logger.error(f"Workflow with UUID '{workflow_uuid}' not found in Galaxy")
        logger.error("Available workflows:")
        for workflow in workflows:
            logger.error(f"  - {workflow.name} (ID: {workflow.id})")
        raise LookupError(f"Workflow with UUID '{workflow_uuid}' not found")
    
    def _find_workflow_by_name(self, workflow_name: str) -> Workflow:
        """
        Find a workflow by name.
        
        Args:
            workflow_name: Name of the workflow to find
            
        Returns:
            Workflow object if found
            
        Raises:
            RuntimeError: If not connected to Galaxy
            LookupError: If workflow is not found
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")

        logger.debug(f"Searching for workflow with name: {workflow_name}")
        # List workflows from Galaxy
        workflows = self.gi.workflows.list()
        
        for workflow in workflows:
            if workflow.name == workflow_name:
                logger.info(f"Found workflow: {workflow.name} (ID: {workflow.id})")
                return workflow
                
        logger.error(f"Workflow with name '{workflow_name}' not found in Galaxy")
        logger.error("Available workflows:")
        for workflow in workflows:
            logger.error(f"  - {workflow.name} (ID: {workflow.id})")
        raise LookupError(f"Workflow with name '{workflow_name}' not found")
    
    def find_workflow(self, workflow_uuid: str) -> Workflow:
        """
        Find a workflow by UUID (public method for backward compatibility).
        
        Args:
            workflow_uuid: UUID of the workflow to find
            
        Returns:
            Workflow object if found
            
        Raises:
            RuntimeError: If not connected to Galaxy
            LookupError: If workflow is not found
        """
        return self._find_workflow_by_uuid(workflow_uuid)
    
    def import_workflow(self, workflow_file_path: Path) -> Workflow:
        """
        Import a workflow from a .ga file.
        
        Args:
            workflow_file_path: Path to the .ga workflow file
            
        Returns:
            Workflow object if import successful
            
        Raises:
            RuntimeError: If not connected to Galaxy
            FileNotFoundError: If workflow file not found
            RuntimeError: If import fails
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")
            
        if not workflow_file_path.exists():
            raise FileNotFoundError(f"Workflow file not found: {workflow_file_path}")
            
        try:
            logger.debug(f"Importing workflow from: {workflow_file_path}")
            
            # Read the workflow file
            with open(workflow_file_path, 'r') as f:
                workflow_data = f.read()
            
            # Import the workflow using the correct BioBlend API
            workflow = self.gi.workflows.import_new(workflow_data)
            
            if workflow:
                logger.info(f"Successfully imported workflow: {workflow.name} (ID: {workflow.id})")
                return workflow
            else:
                raise RuntimeError("Failed to import workflow - no workflow object returned")
                
        except Exception as e:
            if isinstance(e, (RuntimeError, FileNotFoundError)):
                raise
            raise RuntimeError(f"Error importing workflow: {e}") from e
    
    def _load_workflow_registry(self):
        """
        Load workflow registry by scanning the workflows directory and reading .ga files.
        """
        if not self.workflows_path.exists():
            logger.warning(f"Workflows directory does not exist: {self.workflows_path}")
            return
        
        logger.info(f"Loading workflow registry from: {self.workflows_path}")
        
        for workflow_file in self.workflows_path.glob("*.ga"):
            try:
                with open(workflow_file, 'r') as f:
                    workflow_data = json.load(f)
                
                workflow_name = workflow_data.get("name")
                workflow_uuid = workflow_data.get("uuid")
                
                if workflow_name and workflow_uuid:
                    self.workflow_registry[workflow_name] = workflow_uuid
                    logger.debug(f"Registered workflow '{workflow_name}' (UUID: {workflow_uuid}) from {workflow_file.name}")
                else:
                    logger.warning(f"Invalid workflow file {workflow_file.name}: missing name or UUID")
                    
            except (json.JSONDecodeError, IOError) as e:
                logger.error(f"Error reading workflow file {workflow_file.name}: {e}")
    
    def get_workflow_uuid(self, workflow_name: str) -> str:
        """
        Get UUID for a workflow name.
        
        Args:
            workflow_name: Name of the workflow
            
        Returns:
            UUID of the workflow
            
        Raises:
            KeyError: If workflow name is not registered
        """
        if workflow_name not in self.workflow_registry:
            raise KeyError(f"Workflow '{workflow_name}' is not registered. Available workflows: {list(self.workflow_registry.keys())}")
        
        return self.workflow_registry[workflow_name]
    
    def get_available_workflow_files(self, refresh: bool = False) -> Dict[str, str]:
        """
        Get all available workflow names and their UUIDs.
        
        Returns:
            Dictionary mapping workflow names to UUIDs
        """
        if refresh:
            self.workflow_registry = {}
            self._load_workflow_registry()

        return self.workflow_registry.copy()
    
    def get_workflow_structure(self, workflow_name: str) -> Dict[str, Any]:
        """
        Get the workflow structure as a dictionary including steps and connections.
        
        Args:
            workflow_name: Name of the workflow
            
        Returns:
            Dictionary with workflow structure including steps
            
        Raises:
            KeyError: If workflow name is not registered
            RuntimeError: If workflow cannot be accessed
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")
        
        # Get the workflow object
        workflow = self.ensure_workflow_available(workflow_name)
        
        # Use the low-level Galaxy API client to get the full workflow details
        # self.gi.gi is the underlying bioblend.galaxy.GalaxyInstance
        workflow_dict = self.gi.gi.workflows.show_workflow(workflow.id)
        return workflow_dict

    def get_workflow_structure_from_file(self, workflow_name: str) -> Dict[str, Any]:
        """
        Load workflow structure directly from the local .ga file.
        
        This is used as a fallback when Galaxy's API response is missing step UUIDs.
        """
        workflow_uuid = self.get_workflow_uuid(workflow_name)
        workflow_path = self._find_workflow_file_by_uuid(workflow_uuid)
        with open(workflow_path, "r") as f:
            return json.load(f)
    
    def get_workflow_info(self, workflow_name: str) -> Workflow:
        """
        Get detailed information about a workflow including its inputs.
        
        Args:
            workflow_name: Name of the workflow
            
        Returns:
            Workflow object from bioblend
            
        Raises:
            KeyError: If workflow name is not registered
            RuntimeError: If workflow cannot be accessed
        """
        # Ensure workflow is available
        workflow = self.ensure_workflow_available(workflow_name)
        
        return workflow
    
    def _find_workflow_file_by_uuid(self, workflow_uuid: str) -> Path:
        """
        Find workflow file by UUID by scanning all .ga files.
        
        Args:
            workflow_uuid: UUID of the workflow
            
        Returns:
            Path to the workflow file
            
        Raises:
            FileNotFoundError: If workflow file is not found
        """
        for workflow_file in self.workflows_path.glob("*.ga"):
            try:
                with open(workflow_file, 'r') as f:
                    workflow_data = json.load(f)
                
                if workflow_data.get("uuid") == workflow_uuid:
                    logger.debug(f"Found workflow file for UUID {workflow_uuid}: {workflow_file.name}")
                    return workflow_file
                    
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Error reading workflow file {workflow_file.name}: {e}")
                continue
        
        raise FileNotFoundError(f"No workflow file found for UUID '{workflow_uuid}' in {self.workflows_path}")
    
    def _ensure_workflow_exists_by_uuid(self, workflow_uuid: str, force_update: bool = False) -> Workflow:
        """
        Ensure a workflow exists in Galaxy.
        
        Args:
            workflow_uuid: UUID of the workflow to check/import
            force_update: If True, delete and reimport. If False (default), only import if missing.
            
        Returns:
            Workflow object
            
        Raises:
            RuntimeError: If not connected to Galaxy
            LookupError: If workflow file not found
        """
        workflow_file_path = self._find_workflow_file_by_uuid(workflow_uuid)
        
        # Get workflow name from UUID registry
        workflow_name = None
        for name, uuid in self.workflow_registry.items():
            if uuid == workflow_uuid:
                workflow_name = name
                break

        try:
            # Try to find workflow by name (more reliable than UUID)
            existing_workflow = self._find_workflow_by_name(workflow_name) if workflow_name else None

            if existing_workflow and not force_update:
                logger.info(f"Workflow '{workflow_name}' already exists in Galaxy (ID: {existing_workflow.id})")
                return existing_workflow
            elif existing_workflow and force_update:
                # Only if explicitly requested
                logger.warning(f"Force update requested: deleting workflow '{workflow_name}'...")
                existing_workflow.delete()
                logger.info(f"Importing fresh workflow from {workflow_file_path}")
                return self.import_workflow(workflow_file_path)

        except LookupError:
            pass  # Workflow doesn't exist, import it

        # Workflow doesn't exist, import it fresh
        logger.info(f"Workflow '{workflow_name}' not found, importing from {workflow_file_path}")
        return self.import_workflow(workflow_file_path)
    
    def _invoke_workflow_by_uuid(
        self,
        workflow_uuid: str,
        inputs: Dict[str, Any],
        history_name: str = None,
        history_id: str = None,
        preferred_object_store_id: str = None,
        preferred_intermediate_object_store_id: str = None,
        preferred_outputs_object_store_id: str = None,
    ) -> Dict[str, Any]:
        """
        Invoke a workflow by UUID (internal method).
        
        Args:
            workflow_uuid: UUID of the workflow to invoke
            inputs: Dictionary of workflow inputs
            history_name: Optional name for creating a new history
            history_id: Optional existing Galaxy history ID to reuse
            preferred_object_store_id: Optional object store ID for all datasets
            preferred_intermediate_object_store_id: Optional object store for intermediate datasets
            preferred_outputs_object_store_id: Optional object store for marked outputs
            
        Returns:
            Invocation data if successful
            
        Raises:
            RuntimeError: If not connected to Galaxy
            LookupError: If workflow not found
            RuntimeError: If invocation fails
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")
            
        # Find the workflow by name (Galaxy doesn't properly index by UUID)
        # Get workflow name from UUID registry
        workflow_name = None
        for name, uuid in self.workflow_registry.items():
            if uuid == workflow_uuid:
                workflow_name = name
                break
        
        if not workflow_name:
            raise LookupError(f"No workflow found in registry with UUID: {workflow_uuid}")
        
        # Find the workflow by name (more reliable than UUID lookup in Galaxy)
        workflow = self._find_workflow_by_name(workflow_name)
        
        try:
            # Use existing history or create new one
            history = None
            if history_id:
                # Reuse existing history
                history = self.gi.histories.get(history_id)
                logger.info(f"Reusing existing history: {history.name} (ID: {history.id})")
            elif history_name:
                # Create new history
                history = self.gi.histories.create(name=history_name)
                logger.info(f"Created new history: {history.name} (ID: {history.id})")
            
            logger.debug(f"Invoking workflow '{workflow.name}' (UUID: {workflow_uuid}) with inputs: {inputs}")
            
            # Log parameters if present for debugging
            if "parameters" in inputs:
                logger.info(f"Workflow parameters found in inputs: {inputs['parameters']}")
            else:
                logger.warning(f"No parameters found in inputs dict")
            
            # Invoke via low-level API so we can pass object store preferences.
            invocation_payload = self.gi.gi.workflows.invoke_workflow(
                workflow_id=workflow.id,
                inputs=inputs,
                params=inputs.get("parameters"),  # Pass step params separately
                history_id=history.id if history else None,
                history_name=history_name if not history else None,
                preferred_object_store_id=preferred_object_store_id,
                preferred_intermediate_object_store_id=preferred_intermediate_object_store_id,
                preferred_outputs_object_store_id=preferred_outputs_object_store_id,
            )
            invocation_id = invocation_payload["id"]
            invocation_state = invocation_payload.get("state")
            
            logger.info(f"Successfully invoked workflow '{workflow.name}' (Invocation ID: {invocation_id})")
            return {
                "invocation_id": invocation_id,
                "workflow_id": workflow.id,
                "history_id": history.id if history else None,
                "state": invocation_state
            }
            
        except Exception as e:
            if isinstance(e, (RuntimeError, LookupError)):
                raise
            raise RuntimeError(f"Error invoking workflow '{workflow_uuid}': {e}") from e
    
    def ensure_workflow_available(self, workflow_name: str) -> Workflow:
        """
        Ensure a workflow is available in Galaxy (user-facing method).
        
        Args:
            workflow_name: Name of the workflow
            
        Returns:
            Workflow object if workflow is available
            
        Raises:
            KeyError: If workflow name is not registered
            RuntimeError: If workflow cannot be imported
        """
        # Get UUID from registry
        workflow_uuid = self.get_workflow_uuid(workflow_name)
        
        # Ensure workflow exists (will import if needed)
        return self._ensure_workflow_exists_by_uuid(workflow_uuid)
    
    def invoke_workflow(
        self,
        workflow_name: str,
        inputs: Dict[str, Any],
        history_name: str = None,
        history_id: str = None,
        preferred_object_store_id: str = None,
        preferred_intermediate_object_store_id: str = None,
        preferred_outputs_object_store_id: str = None,
    ) -> Dict[str, Any]:
        """
        Invoke a workflow by name (user-facing method).
        
        Args:
            workflow_name: Name of the workflow to invoke
            inputs: Dictionary of workflow inputs
            history_name: Optional name for creating a new history
            history_id: Optional existing Galaxy history ID to reuse
            preferred_object_store_id: Optional object store ID for all datasets
            preferred_intermediate_object_store_id: Optional object store for intermediate datasets
            preferred_outputs_object_store_id: Optional object store for marked outputs
            
        Returns:
            Invocation data if successful
            
        Raises:
            KeyError: If workflow name is not registered
            RuntimeError: If workflow cannot be imported or invoked
        """
        # Get UUID from registry
        workflow_uuid = self.get_workflow_uuid(workflow_name)
        
        # Ensure workflow is available
        self.ensure_workflow_available(workflow_name)
        
        # Invoke by UUID
        return self._invoke_workflow_by_uuid(
            workflow_uuid,
            inputs,
            history_name,
            history_id,
            preferred_object_store_id=preferred_object_store_id,
            preferred_intermediate_object_store_id=preferred_intermediate_object_store_id,
            preferred_outputs_object_store_id=preferred_outputs_object_store_id,
        )
    
    def prepare_workflow_inputs(self, workflow_name: str, dataset_id: str) -> Dict[str, Any]:
        """
        Prepare workflow inputs for a given dataset ID.
        
        Args:
            workflow_name: Name of the workflow
            dataset_id: ID of the dataset in Galaxy history
            
        Returns:
            Dictionary of workflow inputs
            
        Raises:
            KeyError: If workflow name is not registered
        """
        # Get workflow info to understand input structure
        workflow_info = self.get_workflow_info(workflow_name)
        
        # For LAS/LAZ workflows, typically the first input (step 0) is the file
        # This can be made more flexible based on workflow structure
        inputs = {
            "0": {"src": "hda", "id": dataset_id}
        }
        
        return inputs
    
    def prepare_workflow_inputs_from_url(self, workflow_name: str, file_source_uri: str) -> Dict[str, Any]:
        """
        Prepare workflow inputs using a file source URI.
        
        Galaxy will fetch the file directly from the file source as part of
        workflow execution, rather than requiring a pre-existing dataset.
        
        Args:
            workflow_name: Name of the workflow
            file_source_uri: File source URI (e.g., "gxuserfiles://uuid/path/to/file.laz")
            
        Returns:
            Dictionary of workflow inputs with URL source
            
        Raises:
            KeyError: If workflow name is not registered
        """
        # Get workflow info to understand input structure
        workflow_info = self.get_workflow_info(workflow_name)
        
        # Determine file extension from URI
        import os
        ext = os.path.splitext(file_source_uri)[1].lstrip('.').lower()
        if not ext:
            ext = "auto"  # Let Galaxy auto-detect
        
        # Extract filename from URI for the name field
        filename = os.path.basename(file_source_uri)
        
        # For LAS/LAZ workflows, typically the first input (step 0) is the file
        # Galaxy 25.0 expects FileRequestUri format with 'class' field
        # See: https://docs.galaxyproject.org/en/master/_modules/galaxy/schema/schema.html
        inputs = {
            "0": {
                "class": "File",  # Required by Galaxy 25.0
                "url": file_source_uri,
                "ext": ext,
                "name": filename
            }
        }
        
        return inputs
    
    def prepare_collection_inputs_from_storage(
        self, 
        dataset_id: int, 
        supabase_client
    ) -> Dict[str, Any]:
        """
        Build collection input using deferred file-source URIs.
        Galaxy fetches files directly from S3 during job execution.
        
        Args:
            dataset_id: ID of the dataset (parent of dataset_items)
            supabase_client: Supabase client for querying dataset_items
            
        Returns:
            Dict for workflow inputs parameter with collection structure
        """
        # Get all dataset_items for this dataset
        items = supabase_client.client.table("dataset_items")\
            .select("id, bucket_path")\
            .eq("dataset_id", dataset_id)\
            .order("id")\
            .execute()
        
        if not items.data:
            raise ValueError(f"No dataset_items found for dataset_id={dataset_id}")
        
        # Build elements with gxfiles:// URIs
        elements = []
        for item in items.data:
            file_uri = self.get_raw_file_source_uri(item["bucket_path"])
            elements.append({
                "class": "File",
                "identifier": str(item["id"]),  # Element name = item_id
                "location": file_uri,           # gxfiles://raw-storage/RAW/...
                "ext": "laz"
            })
        
        logger.info(f"Built collection input with {len(elements)} elements for dataset_id={dataset_id}")
        
        # Return collection input structure
        return {
            "0": {
                "class": "Collection",
                "collection_type": "list",
                "elements": elements
            }
        }
    
    def invoke_workflow_with_collection(
        self,
        workflow_name: str,
        dataset_id: int,
        supabase_client,
        history_name: str = None,
        history_id: str = None,
        parameters: Dict[str, Any] = None,
        preferred_object_store_id: str = None,
        preferred_intermediate_object_store_id: str = None,
        preferred_outputs_object_store_id: str = None,
    ) -> Dict[str, Any]:
        """
        Invoke workflow with collection input from file sources.
        Galaxy fetches files directly from S3 during job execution.
        
        Args:
            workflow_name: Name of the workflow to invoke
            dataset_id: ID of the dataset (parent of dataset_items)
            supabase_client: Supabase client for querying dataset_items
            history_name: Optional name for creating a new history
            history_id: Optional existing Galaxy history ID to reuse
            parameters: Optional workflow step parameters (e.g., export paths)
            preferred_object_store_id: Optional object store ID for all datasets
            preferred_intermediate_object_store_id: Optional object store for intermediate datasets
            preferred_outputs_object_store_id: Optional object store for marked outputs
            
        Returns:
            Invocation data if successful
            
        Raises:
            KeyError: If workflow name is not registered
            ValueError: If no dataset_items found for dataset_id
            RuntimeError: If workflow cannot be imported or invoked
        """
        logger.info(f"🚀 invoke_workflow_with_collection called: workflow={workflow_name}, dataset_id={dataset_id}, history_id={history_id}")
        
        # Build collection input (no uploads needed - deferred fetch)
        inputs = self.prepare_collection_inputs_from_storage(dataset_id, supabase_client)
        
        # Add step parameters if provided
        if parameters:
            inputs["parameters"] = parameters
        
        # Ensure workflow is available
        self.ensure_workflow_available(workflow_name)
        
        # Invoke workflow - Galaxy handles deferred fetch
        return self.invoke_workflow(
            workflow_name,
            inputs,
            history_name,
            history_id,
            preferred_object_store_id=preferred_object_store_id,
            preferred_intermediate_object_store_id=preferred_intermediate_object_store_id,
            preferred_outputs_object_store_id=preferred_outputs_object_store_id,
        )
    
    def invoke_workflow_with_file_source(
        self, 
        workflow_name: str, 
        file_source_uri: str, 
        history_name: str = None, 
        history_id: str = None,
        parameters: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Invoke a workflow with a file source URI (no pre-import needed).
        
        Galaxy will fetch the file directly from the file source as part of
        workflow execution. This is more efficient than pre-importing the file.
        
        Args:
            workflow_name: Name of the workflow to invoke
            file_source_uri: File source URI (e.g., "gxuserfiles://uuid/path/to/file.laz")
            history_name: Optional name for creating a new history
            history_id: Optional existing Galaxy history ID to reuse
            parameters: Optional workflow step parameters (e.g., export path)
            
        Returns:
            Invocation data if successful
            
        Raises:
            KeyError: If workflow name is not registered
            RuntimeError: If workflow cannot be imported or invoked
        """
        logger.info(f"🚀 invoke_workflow_with_file_source called: workflow={workflow_name}, uri={file_source_uri}, history_id={history_id}, params={parameters}")
        
        # Prepare inputs with file source URL
        inputs = self.prepare_workflow_inputs_from_url(workflow_name, file_source_uri)
        
        # Add parameters if provided (for setting step-specific values like export path)
        if parameters:
            inputs["parameters"] = parameters
        
        # Invoke workflow with existing history or create new one
        return self.invoke_workflow(workflow_name, inputs, history_name, history_id)
    
    def invoke_workflow_with_dataset(self, workflow_name: str, dataset_id: str, history_name: str = None, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Invoke a workflow with an existing dataset in Galaxy.
        
        Args:
            workflow_name: Name of the workflow to invoke
            dataset_id: ID of the dataset in Galaxy history
            history_name: Optional name for the history
            parameters: Optional workflow step parameters (e.g., export path)
            
        Returns:
            Invocation data if successful
            
        Raises:
            KeyError: If workflow name is not registered
            RuntimeError: If workflow cannot be imported or invoked
        """
        logger.info(f"🚀 invoke_workflow_with_dataset called: workflow={workflow_name}, dataset={dataset_id}, params={parameters}")
        
        # Prepare inputs for the workflow
        inputs = self.prepare_workflow_inputs(workflow_name, dataset_id)
        
        # Add parameters if provided (for setting step-specific values like export path)
        if parameters:
            inputs["parameters"] = parameters
        
        # Invoke workflow
        return self.invoke_workflow(workflow_name, inputs, history_name)
    
    def create_history(self, name: str):
        """
        Create a new history.
        
        Args:
            name: Name for the new history
            
        Returns:
            History object
            
        Raises:
            RuntimeError: If not connected to Galaxy
            RuntimeError: If history creation fails
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")
            
        try:
            logger.debug(f"Creating history: {name}")
            history = self.gi.histories.create(name=name)
            logger.info(f"Created history: {history.name} (ID: {history.id})")
            return history
        except Exception as e:
            raise RuntimeError(f"Failed to create history '{name}': {e}") from e

    def delete_history(self, history_id: str, purge: bool = True) -> bool:
        """
        Delete a history from Galaxy.
        
        Args:
            history_id: Galaxy history ID to delete
            purge: If True, permanently purge the history (default). 
                   If False, just mark as deleted.
            
        Returns:
            True if deletion was successful
            
        Raises:
            RuntimeError: If not connected to Galaxy
            RuntimeError: If deletion fails
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")
            
        try:
            logger.info(f"Deleting Galaxy history {history_id} (purge={purge})")
            # Use the low-level API (self.gi.gi) since self.gi is GalaxyObjectsInstance
            # which doesn't have delete_history on ObjHistoryClient
            result = self.gi.gi.histories.delete_history(history_id, purge=purge)
            logger.info(f"Successfully deleted Galaxy history {history_id}: {result}")
            return True
        except Exception as e:
            # Log but don't fail if Galaxy history doesn't exist or is already deleted
            logger.warning(f"Failed to delete Galaxy history {history_id}: {e}")
            return False
    
    def upload_file(self, history, file_path: Path):
        """
        Upload a file to a history.
        
        Args:
            history: History object to upload to
            file_path: Path to the file to upload
            
        Returns:
            Dataset object if successful
            
        Raises:
            RuntimeError: If not connected to Galaxy
            FileNotFoundError: If file does not exist
            RuntimeError: If upload fails
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")
            
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
            
        try:
            logger.info(f"Uploading file: {file_path.name}")
            dataset = history.upload_dataset(str(file_path))
            logger.info(f"Uploaded dataset: {dataset.name} (ID: {dataset.id})")
            return dataset
        except Exception as e:
            raise RuntimeError(f"Failed to upload file '{file_path}': {e}") from e
    
    @staticmethod
    def build_file_source_uri(file_source_id: str, bucket_path: str, scheme: str = "gxfiles") -> str:
        """
        Build a Galaxy file source URI.
        
        Args:
            file_source_id: ID of the file source (e.g., "raw-storage", "products-storage", or UUID for Galaxy EU)
            bucket_path: Path within the bucket (e.g., "LAS/Example_Platane.laz")
            scheme: URI scheme - "gxfiles" for local Galaxy, "gxuserfiles" for Galaxy EU
            
        Returns:
            Complete file source URI (e.g., "gxfiles://raw-storage/LAS/Example_Platane.laz")
            
        Example:
            >>> uri = GalaxyClient.build_file_source_uri("raw-storage", "LAS/Example_Platane.laz")
            >>> print(uri)
            gxfiles://raw-storage/LAS/Example_Platane.laz
            
            # For Galaxy EU:
            >>> uri = GalaxyClient.build_file_source_uri(
            ...     "be5b90f9-ffab-44a2-a1f3-58ba87f04220",
            ...     "LAS/Example_Platane.laz",
            ...     scheme="gxuserfiles"
            ... )
            >>> print(uri)
            gxuserfiles://be5b90f9-ffab-44a2-a1f3-58ba87f04220/LAS/Example_Platane.laz
        """
        # Remove leading slash from bucket_path if present
        bucket_path = bucket_path.lstrip('/')
        return f"{scheme}://{file_source_id}/{bucket_path}"
    
    def import_from_file_source(self, history, file_source_uri: str, file_type: str = "auto"):
        """
        Import a file directly from Galaxy file source (S3/MinIO) into a history.
        
        This method allows Galaxy to read files directly from configured file sources
        (like S3/MinIO) without downloading/uploading through the API server.
        
        Args:
            history: History object to import into
            file_source_uri: URI in format "gxfiles://file-source-id/path/to/file"
                           Example: "gxfiles://raw-storage/LAS/Example_Platane.laz"
            file_type: File type for Galaxy (default: "auto" for auto-detection)
            
        Returns:
            Dataset object with .id attribute if successful
            
        Raises:
            RuntimeError: If not connected to Galaxy
            ValueError: If URI format is invalid
            RuntimeError: If import fails
            
        Example:
            >>> dataset = galaxy_client.import_from_file_source(
            ...     history=history,
            ...     file_source_uri="gxfiles://raw-storage/LAS/Example_Platane.laz"
            ... )
            >>> print(dataset.id)  # Use in workflow invocation
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")
        
        # Accept gxfiles:// (local Galaxy), gxuserfiles:// (Galaxy EU), or direct HTTPS URLs
        valid_schemes = ("gxfiles://", "gxuserfiles://", "https://", "http://")
        if not any(file_source_uri.startswith(scheme) for scheme in valid_schemes):
            raise ValueError(
                f"Invalid file source URI: {file_source_uri}. "
                "Must start with 'gxfiles://', 'gxuserfiles://', or 'https://'"
            )
        
        try:
            logger.info(f"Importing from file source: {file_source_uri}")
            
            # Use Galaxy's fetch API to import from file source
            # POST /api/tools/fetch with targets containing the gxfiles:// URI
            payload = {
                "history_id": history.id,
                "targets": [{
                    "destination": {"type": "hdas"},
                    "elements": [{
                        "src": "url",
                        "url": file_source_uri,
                        "ext": file_type if file_type != "auto" else "auto"
                    }]
                }]
            }
            
            # Use bioblend's make_post_request to call the API
            url = f"{self.url}/api/tools/fetch"
            response = self.gi.gi.make_post_request(url, payload)
            
            # Extract dataset from response
            if not response or not isinstance(response, dict):
                raise RuntimeError(f"Import failed: Invalid response from Galaxy: {response}")
            
            # The response should contain outputs
            outputs = response.get('outputs', [])
            if not outputs:
                raise RuntimeError(f"Import failed: No outputs in response: {response}")
            
            dataset_info = outputs[0]
            dataset_id = dataset_info.get('id')
            if not dataset_id:
                raise RuntimeError(f"Import failed: No dataset ID in response: {response}")
            
            # Get the full dataset object
            dataset = self.gi.datasets.get(dataset_id)
            logger.info(f"Imported dataset from file source: {dataset.name} (ID: {dataset.id})")
            
            return dataset
            
        except Exception as e:
            if isinstance(e, (RuntimeError, ValueError)):
                raise
            raise RuntimeError(f"Failed to import from file source '{file_source_uri}': {e}") from e
    
    def wait_for_upload(self, dataset, timeout: int = 300) -> bool:
        """
        Wait for a dataset upload to complete.
        
        Args:
            dataset: Dataset object to monitor
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if upload completed successfully
            
        Raises:
            RuntimeError: If upload fails or times out
        """
        logger.debug("Waiting for upload to complete...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            dataset.refresh()
            if dataset.state == 'ok':
                logger.info("Upload completed successfully")
                return True
            elif dataset.state in ['error', 'failed_metadata']:
                raise RuntimeError(f"Upload failed with state: {dataset.state}")
            time.sleep(2)
            
        raise RuntimeError(f"Upload timeout after {timeout} seconds")
    
    def get_workflow_invocations(self, workflow_id: Optional[str] = None, invocation_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get workflow invocations from Galaxy.
        
        Args:
            workflow_id: Optional workflow ID to filter invocations
            invocation_ids: Optional list of specific invocation IDs to get
            
        Returns:
            List of invocation data dictionaries with separated fields
            
        Raises:
            RuntimeError: If not connected to Galaxy
            RuntimeError: If Galaxy API call fails
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")
            
        logger.debug("Getting workflow invocations from Galaxy...")
        
        # Check if invocations API is available
        if not hasattr(self.gi, 'invocations'):
            logger.warning("Galaxy invocations API not available, returning empty list")
            return []
        
        # Get invocations with step_details=true to get all jobs (including collection/map-over jobs)
        invocations = []
        if invocation_ids:
            for inv_id in invocation_ids:
                try:
                    inv = self.gi.gi.make_get_request(
                        f"{self.gi.gi.url}/invocations/{inv_id}",
                        params={"step_details": "true"}
                    ).json()
                    if inv:
                        invocations.append(inv)
                except Exception as e:
                    logger.warning(f"Could not get invocation {inv_id}: {e}")
        else:
            try:
                invocations = self.gi.invocations.get_invocations(
                    workflow_id=workflow_id,
                    step_details=True
                )
            except Exception as e:
                raise RuntimeError(f"Failed to list workflow invocations: {e}") from e
        
        # Convert to standardized format
        invocation_data = []
        for inv in invocations:
            # Extract all jobs from steps
            jobs = []
            for step in inv.get('steps', []):
                for job in step.get('jobs', []):
                    jobs.append({
                        'id': job.get('id'),
                        'state': job.get('state', 'unknown'),
                        'tool_id': job.get('tool_id', ''),
                        'update_time': job.get('update_time', '')
                    })
            
            invocation_data.append({
                "id": inv.get('id'),
                "workflow_id": inv.get('workflow_id'),
                "state": inv.get('state'),
                "history_id": inv.get('history_id'),
                "update_time": inv.get('update_time', ''),
                "steps": inv.get('steps', []),
                "inputs": inv.get('inputs', {}),
                "outputs": inv.get('outputs', {}),
                "output_collections": inv.get('output_collections', {}),
                "jobs": jobs,
                "messages": inv.get('messages', [])
            })
        
        logger.info(f"Retrieved {len(invocation_data)} workflow invocations")
        return invocation_data
    
    def get_job_details(self, job_id: str) -> Dict[str, Any]:
        """
        Get detailed job information including stderr, exit_code, etc.
        
        This is used for diagnostic information when jobs fail.
        Requires the 'full' parameter to get stderr/stdout.
        
        Args:
            job_id: Galaxy job ID
            
        Returns:
            Dictionary with job details including:
            - exit_code: Process exit code (139=SIGSEGV, 137=OOM, etc.)
            - tool_stderr: Error output from the tool
            - tool_stdout: Standard output from the tool
            - job_messages: Galaxy-level error messages
            - state: Job state
            - tool_id: Tool identifier
            
        Raises:
            RuntimeError: If not connected to Galaxy
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")
        
        try:
            response = self.gi.gi.make_get_request(
                f"{self.gi.gi.url}/jobs/{job_id}",
                params={"full": "true"}
            )
            job_data = response.json()
            
            # Extract relevant fields
            return {
                "id": job_data.get("id"),
                "state": job_data.get("state"),
                "tool_id": job_data.get("tool_id"),
                "exit_code": job_data.get("exit_code"),
                "tool_stderr": job_data.get("tool_stderr", ""),
                "tool_stdout": job_data.get("tool_stdout", ""),
                "job_messages": [m.get("message", str(m)) for m in job_data.get("job_messages", [])],
                "create_time": job_data.get("create_time"),
                "update_time": job_data.get("update_time"),
            }
        except Exception as e:
            logger.warning(f"Could not get job details for {job_id}: {e}")
            return {
                "id": job_id,
                "state": "unknown",
                "tool_id": "",
                "exit_code": None,
                "tool_stderr": "",
                "tool_stdout": "",
                "job_messages": [],
            }
    
    def _serialize_step(self, step) -> Dict[str, Any]:
        """
        Serialize a workflow step by filtering out non-serializable attributes.
        
        Args:
            step: BioBlend workflow step object
            
        Returns:
            Dictionary with serializable step data
        """
        if not hasattr(step, '__dict__'):
            return step
            
        step_dict = step.__dict__.copy()
        
        # Remove non-serializable attributes
        non_serializable = ['_cached_parent', 'is_modified', 'gi']
        for attr in non_serializable:
            step_dict.pop(attr, None)
            
        return step_dict
    
    def get_invocation_details(self, invocation_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific workflow invocation.
        
        Args:
            invocation_id: ID of the invocation to get details for
            
        Returns:
            Dictionary with detailed invocation information
            
        Raises:
            RuntimeError: If not connected to Galaxy
            RuntimeError: If Galaxy API call fails
            LookupError: If invocation not found
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")
            
        logger.debug(f"Getting details for invocation: {invocation_id}")
        
        # Check if invocations API is available
        if not hasattr(self.gi, 'invocations'):
            raise RuntimeError("Galaxy invocations API not available")
        
        # Get invocation details
        try:
            invocation = self.gi.invocations.get(invocation_id)
        except Exception as e:
            raise RuntimeError(f"Failed to get invocation {invocation_id} from Galaxy: {e}") from e
        
        if not invocation:
            raise LookupError(f"Invocation {invocation_id} not found")
        
        # Get history details
        history = None
        if invocation.history_id:
            try:
                history = self.gi.histories.get(invocation.history_id)
            except Exception as e:
                logger.warning(f"Could not get history {invocation.history_id}: {e}")
        
        return {
            "id": invocation.id,
            "workflow_id": invocation.workflow_id,
            "state": invocation.state,
            "history_id": invocation.history_id,
            "history_name": history.name if history else None,
            "update_time": invocation.update_time,
            "inputs": getattr(invocation, 'inputs', {}),
            "outputs": getattr(invocation, 'outputs', {}),
            "steps": [self._serialize_step(step) for step in getattr(invocation, 'steps', [])]
        }
    
    def get_history_datasets(self, history_id: str) -> List[Dict[str, Any]]:
        """
        Get all datasets from a Galaxy history.
        
        Args:
            history_id: ID of the history to get datasets from
            
        Returns:
            List of dataset information dictionaries
            
        Raises:
            RuntimeError: If not connected to Galaxy
            RuntimeError: If Galaxy API call fails
            LookupError: If history not found
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")
            
        logger.debug(f"Getting datasets from history: {history_id}")
        
        # Get history
        try:
            history = self.gi.histories.get(history_id)
        except Exception as e:
            raise RuntimeError(f"Failed to get history {history_id} from Galaxy: {e}") from e
        
        if not history:
            raise LookupError(f"History {history_id} not found")
        
        # Get datasets from history
        try:
            datasets = history.get_datasets()
        except Exception as e:
            raise RuntimeError(f"Failed to get datasets from history {history_id}: {e}") from e
        
        dataset_data = []
        for dataset in datasets:
            dataset_data.append({
                "id": dataset.id,
                "name": dataset.name,
                "state": dataset.state,
                "file_size": getattr(dataset, 'file_size', 0),
                "file_ext": getattr(dataset, 'file_ext', ''),
                "created_at": getattr(dataset, 'create_time', None),
                "updated_at": getattr(dataset, 'update_time', None),
                "download_url": f"{self.url}/api/histories/{history_id}/contents/{dataset.id}/display"
            })
        
        logger.info(f"Retrieved {len(dataset_data)} datasets from history {history_id}")
        return dataset_data
    
    def download_dataset(self, history_id: str, dataset_id: str, output_path: Path) -> bool:
        """
        Download a dataset from Galaxy history.
        
        Args:
            history_id: ID of the history containing the dataset
            dataset_id: ID of the dataset to download
            output_path: Local path to save the dataset
            
        Returns:
            True if download successful
            
        Raises:
            RuntimeError: If not connected to Galaxy
            RuntimeError: If Galaxy API call fails
            LookupError: If dataset not found
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")
            
        logger.debug(f"Downloading dataset {dataset_id} from history {history_id}")
        
        # Get the dataset
        try:
            dataset = self.gi.datasets.get(dataset_id)
        except Exception as e:
            raise RuntimeError(f"Failed to get dataset {dataset_id} from Galaxy: {e}") from e
        
        if not dataset:
            raise LookupError(f"Dataset {dataset_id} not found")
        
        # Download the dataset
        try:
            with open(output_path, 'wb') as f:
                dataset.download(f)
        except Exception as e:
            raise RuntimeError(f"Failed to download dataset {dataset_id} to {output_path}: {e}") from e
        
        logger.info(f"Successfully downloaded dataset {dataset_id} to {output_path}")
        return True
    