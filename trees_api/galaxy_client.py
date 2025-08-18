import json
import os
import time
import base64
import logging
import requests
from pathlib import Path
from typing import Optional, Dict, Any, List

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from bioblend.galaxy.objects import GalaxyInstance as GalaxyObjectsInstance

logger = logging.getLogger("trees_api.galaxy_client")


class GalaxyClient(BaseSettings):
    url: str = Field(default='http://127.0.0.1:9090', description="Galaxy server URL")
    api_key: Optional[str] = Field(default=None, description="Galaxy API key (if already available)")
    email: Optional[str] = Field(default='processor@3dtrees.earth', description="Galaxy user email")
    password: Optional[str] = Field(default=None, description="Galaxy user password")
    admin_key: Optional[str] = Field(default=None, description="Galaxy admin key")
    workflows_path: Path = Field(default=Path(__file__).parent / "workflows", description="Path to workflow files")
    
    workflow_registry: Dict[str, str] = Field(default_factory=dict, init=False)
    gi: Optional[GalaxyObjectsInstance] = Field(default=None, init=False)

    model_config = SettingsConfigDict(
        case_sensitive=False,
        cli_parse_args=True,
        cli_ignore_unknown_args=True,
        env_file = ".env",
        env_prefix = "GALAXY_",
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
        
        # If baseauth fails, try to create user
        logger.debug(f"Attempting to create user {email}...")
        try:
            # Try to register the user
            register_data = {
                "email": email,
                "password": password,
                "username": email.split('@')[0],  # Use email prefix as username
                "confirm": password
            }
            
            response = requests.post(f"{self.url}/api/users", json=register_data)
            if response.status_code in [200, 201]:
                logger.info(f"User {email} created successfully")
            elif response.status_code == 400:
                # User might already exist, try login
                logger.debug(f"User {email} might already exist, trying login...")
            else:
                raise RuntimeError(f"User creation failed with status {response.status_code}")
            
            # Now try to get API key again
            response = requests.get(f"{self.url}/api/authenticate/baseauth", headers=headers)
            if response.status_code == 200:
                api_key = response.json().get('api_key')
                if api_key:
                    logger.info(f"Successfully authenticated and got API key for {email}")
                    return api_key
            
        except Exception as e:
            if isinstance(e, RuntimeError):
                raise
            logger.debug(f"Error during user creation/login: {e}")
        
        raise RuntimeError(f"Failed to authenticate user {email} and get API key")
    
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
    
    def _find_workflow_by_uuid(self, workflow_uuid: str):
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
        workflows = self.gi.workflows.list()
        
        for workflow in workflows:
            if workflow.id == workflow_uuid:
                logger.info(f"Found workflow: {workflow.name} (ID: {workflow.id})")
                return workflow
                
        logger.error(f"Workflow with UUID '{workflow_uuid}' not found in Galaxy")
        logger.error("Available workflows:")
        for workflow in workflows:
            logger.error(f"  - {workflow.name} (ID: {workflow.id})")
        raise LookupError(f"Workflow with UUID '{workflow_uuid}' not found")
    
    def _find_workflow_by_name(self, workflow_name: str):
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
    
    def find_workflow(self, workflow_uuid: str):
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
    
    def import_workflow(self, workflow_file_path: Path):
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
    
    def get_available_workflows(self, refresh: bool = False) -> Dict[str, str]:
        """
        Get all available workflow names and their UUIDs.
        
        Returns:
            Dictionary mapping workflow names to UUIDs
        """
        if refresh:
            self.workflow_registry = {}
            self._load_workflow_registry()

        return self.workflow_registry.copy()
    
    def get_workflow_info(self, workflow_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a workflow including its inputs.
        
        Args:
            workflow_name: Name of the workflow
            
        Returns:
            Dictionary with workflow information including inputs
            
        Raises:
            KeyError: If workflow name is not registered
            RuntimeError: If workflow cannot be accessed
        """
        # Get UUID from registry
        workflow_uuid = self.get_workflow_uuid(workflow_name)
        
        # Ensure workflow is available
        workflow = self.ensure_workflow_available(workflow_name)
        
        return {
            "name": workflow.name,
            "id": workflow.id,
            "uuid": workflow_uuid,
            "inputs": workflow.inputs,
            "annotation": workflow.annotation,
            "tags": workflow.tags,
            "version": workflow.version
        }
    
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
    
    def _ensure_workflow_exists_by_uuid(self, workflow_uuid: str):
        """
        Ensure a workflow exists in Galaxy, import it if it doesn't (internal method).
        
        Args:
            workflow_uuid: UUID of the workflow to check/import
            
        Returns:
            Workflow object if workflow exists or was imported successfully
            
        Raises:
            RuntimeError: If not connected to Galaxy
            LookupError: If workflow not found and import fails
        """
        try:
            # First check if workflow already exists
            workflow = self._find_workflow_by_uuid(workflow_uuid)
            logger.info(f"Workflow with UUID '{workflow_uuid}' already exists in Galaxy")
            return workflow
        except LookupError:
            # Workflow doesn't exist, import it
            workflow_file_path = self._find_workflow_file_by_uuid(workflow_uuid)
            logger.info(f"Workflow with UUID '{workflow_uuid}' not found, importing from {workflow_file_path}")
            return self.import_workflow(workflow_file_path)
    
    def _invoke_workflow_by_uuid(self, workflow_uuid: str, inputs: Dict[str, Any], history_name: str = None) -> Dict[str, Any]:
        """
        Invoke a workflow by UUID (internal method).
        
        Args:
            workflow_uuid: UUID of the workflow to invoke
            inputs: Dictionary of workflow inputs
            history_name: Optional name for the history
            
        Returns:
            Invocation data if successful
            
        Raises:
            RuntimeError: If not connected to Galaxy
            LookupError: If workflow not found
            RuntimeError: If invocation fails
        """
        if not self.gi:
            raise RuntimeError("Not connected to Galaxy. Call connect() first.")
            
        # Find the workflow
        workflow = self._find_workflow_by_uuid(workflow_uuid)
        
        try:
            # Create a history if name provided
            history = None
            if history_name:
                history = self.gi.histories.create(name=history_name)
                logger.info(f"Created history: {history.name} (ID: {history.id})")
            
            logger.debug(f"Invoking workflow '{workflow.name}' (UUID: {workflow_uuid}) with inputs: {inputs}")
            
            # Invoke the workflow
            invocation = workflow.invoke(
                inputs=inputs,
                history=history
            )
            
            logger.info(f"Successfully invoked workflow '{workflow.name}' (Invocation ID: {invocation.id})")
            return {
                "invocation_id": invocation.id,
                "workflow_id": workflow.id,
                "history_id": history.id if history else None,
                "state": invocation.state
            }
            
        except Exception as e:
            if isinstance(e, (RuntimeError, LookupError)):
                raise
            raise RuntimeError(f"Error invoking workflow '{workflow_uuid}': {e}") from e
    
    def ensure_workflow_available(self, workflow_name: str):
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
    
    def invoke_workflow(self, workflow_name: str, inputs: Dict[str, Any], history_name: str = None) -> Dict[str, Any]:
        """
        Invoke a workflow by name (user-facing method).
        
        Args:
            workflow_name: Name of the workflow to invoke
            inputs: Dictionary of workflow inputs
            history_name: Optional name for the history
            
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
        return self._invoke_workflow_by_uuid(workflow_uuid, inputs, history_name)
    
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
    
    def invoke_workflow_with_dataset(self, workflow_name: str, dataset_id: str, history_name: str = None) -> Dict[str, Any]:
        """
        Invoke a workflow with an existing dataset in Galaxy.
        
        Args:
            workflow_name: Name of the workflow to invoke
            dataset_id: ID of the dataset in Galaxy history
            history_name: Optional name for the history
            
        Returns:
            Invocation data if successful
            
        Raises:
            KeyError: If workflow name is not registered
            RuntimeError: If workflow cannot be imported or invoked
        """
        # Prepare inputs for the workflow
        inputs = self.prepare_workflow_inputs(workflow_name, dataset_id)
        
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
    