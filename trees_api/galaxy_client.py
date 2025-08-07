"""
Galaxy client module for 3DTrees API.
Handles authentication, tool execution, and job monitoring.
"""

import os
import time
import base64
import logging
import requests
from pathlib import Path
from typing import Optional, Dict, Any, List

from bioblend.galaxy.objects import GalaxyInstance as GalaxyObjectsInstance

logger = logging.getLogger(__name__)


class GalaxyClient:
    """Client for interacting with Galaxy API using BioBlend."""
    
    def __init__(self, galaxy_url: str, api_key: Optional[str] = None):
        """
        Initialize Galaxy client.
        
        Args:
            galaxy_url: URL of the Galaxy instance
            api_key: Optional API key. If not provided, will attempt authentication
        """
        self.galaxy_url = galaxy_url
        self.api_key = api_key
        self.gi = None
        
    def authenticate(self, email: str, password: str) -> bool:
        """
        Authenticate with Galaxy and get API key.
        
        Args:
            email: User email
            password: User password
            
        Returns:
            True if authentication successful, False otherwise
        """
        if self.api_key:
            logger.debug("Using existing API key")
            return True
            
        logger.info("No API key found, attempting authentication...")
        api_key = self._get_or_create_api_key(email, password)
        
        if not api_key:
            logger.error("Failed to authenticate and get API key")
            return False
            
        self.api_key = api_key
        # Store the API key in environment variable for future use
        os.environ["GALAXY_API_KEY"] = api_key
        logger.debug("API key stored in environment variable")
        return True
    
    def _get_or_create_api_key(self, email: str, password: str) -> Optional[str]:
        """
        Get API key using baseauth, or create user if it doesn't exist.
        Based on Galaxy API documentation: https://galaxyproject.org/develop/api/
        """
        # First try to get API key using baseauth
        auth_string = base64.b64encode(f"{email}:{password}".encode()).decode()
        headers = {"Authorization": f"Basic {auth_string}"}
        
        try:
            response = requests.get(f"{self.galaxy_url}/api/authenticate/baseauth", headers=headers)
            if response.status_code == 200:
                api_key = response.json().get('api_key')
                if api_key:
                    logger.info(f"Successfully authenticated and got API key for {email}")
                    return api_key
        except Exception as e:
            logger.error(f"Baseauth failed: {e}")
        
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
            
            response = requests.post(f"{self.galaxy_url}/api/users", json=register_data)
            if response.status_code in [200, 201]:
                logger.info(f"User {email} created successfully")
            elif response.status_code == 400:
                # User might already exist, try login
                logger.debug(f"User {email} might already exist, trying login...")
            else:
                logger.error(f"User creation failed with status {response.status_code}")
                return None
            
            # Now try to get API key again
            response = requests.get(f"{self.galaxy_url}/api/authenticate/baseauth", headers=headers)
            if response.status_code == 200:
                api_key = response.json().get('api_key')
                if api_key:
                    logger.info(f"Successfully authenticated and got API key for {email}")
                    return api_key
            
        except Exception as e:
            logger.error(f"Error during user creation/login: {e}")
        
        return None
    
    def connect(self) -> bool:
        """
        Connect to Galaxy instance.
        
        Returns:
            True if connection successful, False otherwise
        """
        if not self.api_key:
            logger.error("No API key available. Call authenticate() first.")
            return False
            
        try:
            logger.debug(f"Connecting to Galaxy at {self.galaxy_url}...")
            self.gi = GalaxyObjectsInstance(self.galaxy_url, self.api_key)
            
            # Test connection
            version_info = self.gi.gi.make_get_request(f"{self.galaxy_url}/api/version").json()
            logger.info(f"Connected to Galaxy version: {version_info.get('version_major', 'Unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Galaxy: {e}")
            return False
    
    def find_tool(self, tool_name: str):
        """
        Find a tool by name.
        
        Args:
            tool_name: Name of the tool to find
            
        Returns:
            Tool object if found, None otherwise
        """
        if not self.gi:
            logger.error("Not connected to Galaxy. Call connect() first.")
            return None
            
        logger.debug(f"Searching for tool: {tool_name}")
        tools = self.gi.tools.list()
        
        for tool in tools:
            if tool.name == tool_name:
                logger.info(f"Found tool: {tool.name}")
                return tool
                
        logger.error(f"Tool '{tool_name}' not found in Galaxy")
        logger.error("Available tools:")
        for tool in tools[:10]:  # Show first 10 tools
            logger.error(f"  - {tool.name}")
        if len(tools) > 10:
            logger.error(f"  ... and {len(tools) - 10} more")
        return None
    
    def create_history(self, name: str):
        """
        Create a new history.
        
        Args:
            name: Name for the new history
            
        Returns:
            History object
        """
        if not self.gi:
            logger.error("Not connected to Galaxy. Call connect() first.")
            return None
            
        logger.debug(f"Creating history: {name}")
        history = self.gi.histories.create(name=name)
        logger.info(f"Created history: {history.name} (ID: {history.id})")
        return history
    
    def upload_file(self, history, file_path: Path) -> Optional[Any]:
        """
        Upload a file to a history.
        
        Args:
            history: History object to upload to
            file_path: Path to the file to upload
            
        Returns:
            Dataset object if successful, None otherwise
        """
        if not self.gi:
            logger.error("Not connected to Galaxy. Call connect() first.")
            return None
            
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return None
            
        logger.info(f"Uploading file: {file_path.name}")
        dataset = history.upload_dataset(str(file_path))
        logger.info(f"Uploaded dataset: {dataset.name} (ID: {dataset.id})")
        return dataset
    
    def wait_for_upload(self, dataset, timeout: int = 300) -> bool:
        """
        Wait for a dataset upload to complete.
        
        Args:
            dataset: Dataset object to monitor
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if upload completed successfully, False otherwise
        """
        logger.debug("Waiting for upload to complete...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            dataset.refresh()
            if dataset.state == 'ok':
                logger.info("Upload completed successfully")
                return True
            elif dataset.state in ['error', 'failed_metadata']:
                logger.error(f"Upload failed with state: {dataset.state}")
                return False
            time.sleep(2)
            
        logger.error(f"Upload timeout after {timeout} seconds")
        return False
    
    def run_tool(self, tool, history, tool_inputs: Dict[str, Any]):
        """
        Run a tool with given inputs.
        
        Args:
            tool: Tool object to run
            history: History object to run in
            tool_inputs: Dictionary of tool input parameters
            
        Returns:
            Job result
        """
        if not self.gi:
            logger.error("Not connected to Galaxy. Call connect() first.")
            return None
            
        logger.debug("Invoking tool...")
        logger.debug(f"Tool inputs: {tool_inputs}")
        job = tool.run(tool_inputs, history)
        logger.info("Tool invocation started")
        logger.debug(f"Job: {job}")
        return job
    
    def wait_for_job_completion(self, history, input_dataset_name: str, timeout: int = 1800) -> bool:
        """
        Wait for a job to complete and produce output datasets.
        
        Args:
            history: History object to monitor
            input_dataset_name: Name of the input dataset to exclude from outputs
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if job completed successfully, False otherwise
        """
        logger.debug("Monitoring job progress...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            history.refresh()
            datasets = history.get_datasets()
            
            # Check if we have new output datasets
            output_datasets = [d for d in datasets if d.name != input_dataset_name]
            
            if output_datasets:
                logger.info("Job completed successfully!")
                return True
            
            # Check if any datasets are in error state
            error_datasets = [d for d in datasets if d.state in ['error', 'failed_metadata']]
            if error_datasets:
                logger.error(f"Job failed with error state: {error_datasets[0].state}")
                return False
            
            logger.debug("Job is running...")
            time.sleep(5)
            
        logger.error(f"Job timeout after {timeout} seconds")
        return False
    
    def get_job_results(self, history) -> List[Dict[str, Any]]:
        """
        Get results from a completed job.
        
        Args:
            history: History object to get results from
            
        Returns:
            List of dataset information dictionaries
        """
        logger.debug("Getting job results...")
        history.refresh()
        datasets = history.get_datasets()
        
        results = []
        for dataset in datasets:
            results.append({
                'name': dataset.name,
                'file_ext': dataset.file_ext,
                'state': dataset.state,
                'id': dataset.id
            })
            
        logger.info(f"Job completed! History contains {len(datasets)} datasets:")
        for result in results:
            logger.info(f"  - {result['name']} ({result['file_ext']}) - State: {result['state']}")
            
        return results 