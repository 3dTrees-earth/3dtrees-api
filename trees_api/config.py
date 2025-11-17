"""
Centralized configuration for 3DTrees API.

All configuration is loaded from environment variables with sensible defaults.
This module provides a single source of truth for all service configurations.
"""
from typing import Optional
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GalaxyConfig(BaseSettings):
    """Galaxy server configuration."""
    
    url: str = Field(default='http://127.0.0.1:9090', description="Galaxy server URL")
    api_key: Optional[str] = Field(default=None, description="Galaxy API key (if already available)")
    email: Optional[str] = Field(default='processor@3dtrees.earth', description="Galaxy user email")
    password: Optional[str] = Field(default=None, description="Galaxy user password")
    admin_key: Optional[str] = Field(default=None, description="Galaxy admin key")
    workflows_path: Path = Field(
        default_factory=lambda: Path(__file__).parent / "workflows",
        description="Path to workflow files"
    )
    
    model_config = SettingsConfigDict(
        case_sensitive=False,
        cli_parse_args=True,
        cli_ignore_unknown_args=True,
        env_prefix="GALAXY_",
        extra="ignore",
    )


class SupabaseConfig(BaseSettings):
    """Supabase database configuration."""
    
    url: str = Field(default="", description="Supabase project URL")
    key: str = Field(default="", description="Supabase anon/public key")
    service_key: Optional[str] = Field(default=None, description="Supabase service role key (for admin operations)")
    email: Optional[str] = Field(default="processor@3dtrees.earth", description="Supabase user email")
    password: Optional[str] = Field(default=None, description="Supabase user password")
    datasets_table: str = Field(default="datasets", description="Supabase datasets table name")
    invocations_table: str = Field(default="galaxy_workflow_invocations", description="Supabase workflow invocations table name")
    
    model_config = SettingsConfigDict(
        case_sensitive=False,
        cli_parse_args=True,
        cli_ignore_unknown_args=True,
        env_prefix="SUPABASE_",
        extra="ignore",
    )


class StorageConfig(BaseSettings):
    """S3/MinIO storage configuration."""
    
    access_key: str = Field(description="Storage access key")
    secret_key: str = Field(description="Storage secret key")
    bucket_name: str = Field(default="3dtrees-tool-dev", description="Legacy single bucket name")
    bucket_name_products: str = Field(default="3dtrees-tool-products", description="Products bucket name")
    bucket_name_raw: str = Field(default="3dtrees-tool-raw", description="Raw data bucket name")
    url: str = Field(default="https://storage.googleapis.com", description="Storage endpoint URL")
    region: str = Field(default="eu", description="Storage region")
    
    model_config = SettingsConfigDict(
        case_sensitive=False,
        cli_parse_args=True,
        cli_ignore_unknown_args=True,
        env_prefix="STORAGE_",
        extra="ignore",
    )


class APIConfig(BaseSettings):
    """API server configuration."""
    
    host: str = Field(default="0.0.0.0", description="API server host")
    port: int = Field(default=8000, description="API server port")
    reload: bool = Field(default=False, description="Enable auto-reload for development")
    
    model_config = SettingsConfigDict(
        case_sensitive=False,
        cli_parse_args=True,
        cli_ignore_unknown_args=True,
        env_prefix="API_SERVER_",
        extra="ignore",
    )


class AppConfig:
    """
    Aggregated configuration with validation on startup.
    
    This class instantiates all sub-configurations and provides a centralized
    place to validate that all required settings are present.
    """
    
    def __init__(self):
        self.galaxy = GalaxyConfig()
        self.supabase = SupabaseConfig()
        self.storage = StorageConfig()
        self.api = APIConfig()
    
    def validate(self) -> None:
        """
        Validate all configurations are set correctly.
        
        Raises:
            ValueError: If any required configuration is missing or invalid
        """
        errors = []
        
        # Validate Supabase
        if not self.supabase.url:
            errors.append("SUPABASE_URL is required")
        if not self.supabase.key:
            errors.append("SUPABASE_KEY is required")
        
        # Validate Storage
        if not self.storage.access_key:
            errors.append("STORAGE_ACCESS_KEY is required")
        if not self.storage.secret_key:
            errors.append("STORAGE_SECRET_KEY is required")
        
        # Validate Galaxy (email+password OR api_key required for auth)
        if not self.galaxy.api_key:
            if not self.galaxy.email or not self.galaxy.password:
                errors.append("Either GALAXY_API_KEY or both GALAXY_EMAIL and GALAXY_PASSWORD are required")
        
        if errors:
            raise ValueError(f"Configuration errors: {'; '.join(errors)}")

