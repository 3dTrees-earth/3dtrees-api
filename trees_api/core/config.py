"""
Centralized configuration for 3DTrees API.

All configuration is loaded from environment variables with sensible defaults.
This module provides a single source of truth for all service configurations.
"""

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GalaxyConfig(BaseSettings):
    """Galaxy server configuration.

    File Source Configuration:
        Local Galaxy uses simple IDs: raw-storage, products-storage, visualization-storage
        Galaxy EU uses UUIDs: gxuserfiles://<uuid>/

        Set these environment variables for Galaxy EU:
            GALAXY_FILE_SOURCE_RAW=be5b90f9-ffab-44a2-a1f3-58ba87f04220
            GALAXY_FILE_SOURCE_PRODUCTS=e1d3f62b-2abb-4f1f-a888-e69f676980cb
            GALAXY_FILE_SOURCE_VISUALIZATION=b3593d8c-534a-428b-bdc1-891cfc7e0547
            GALAXY_FILE_SOURCE_SCHEME=gxuserfiles
    """

    url: str = Field(default="http://127.0.0.1:9090", description="Galaxy server URL")
    api_key: Optional[str] = Field(default=None, description="Galaxy API key (if already available)")
    email: Optional[str] = Field(default="processor@3dtrees.earth", description="Galaxy user email")
    password: Optional[str] = Field(default=None, description="Galaxy user password")
    admin_key: Optional[str] = Field(default=None, description="Galaxy admin key")
    workflows_path: Optional[Path] = Field(default=None, description="Override path to workflow files")
    workflows_path_dev: Path = Field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "workflows",
        description="Path to development workflow files",
    )
    workflows_path_prod: Path = Field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "workflows_prod",
        description="Path to production workflow files",
    )

    file_source_raw: str = Field(default="raw-storage", description="Galaxy file source ID for raw data bucket")
    file_source_products: str = Field(default="products-storage", description="Galaxy file source ID for products bucket")
    file_source_visualization: str = Field(
        default="visualization-storage",
        description="Galaxy file source ID for visualization bucket (3dtiles, overviews)",
    )
    file_source_scheme: str = Field(
        default="gxfiles",
        description="URI scheme for file sources (gxfiles for local, gxuserfiles for Galaxy EU)",
    )
    default_object_store_id: Optional[str] = Field(default=None, description="Default Galaxy object store ID for workflow invocations")
    default_intermediate_object_store_id: Optional[str] = Field(
        default=None, description="Default Galaxy object store ID for intermediate datasets"
    )
    default_outputs_object_store_id: Optional[str] = Field(
        default=None, description="Default Galaxy object store ID for marked workflow outputs"
    )
    model_config = SettingsConfigDict(
        case_sensitive=False,
        cli_parse_args=True,
        cli_ignore_unknown_args=True,
        env_prefix="GALAXY_",
        extra="ignore",
    )

    def build_file_source_uri(self, file_source_id: str, path: str) -> str:
        path = path.lstrip("/")
        return f"{self.file_source_scheme}://{file_source_id}/{path}"

    @property
    def is_galaxy_eu(self) -> bool:
        return "usegalaxy.eu" in self.url or self.file_source_scheme == "gxuserfiles"

    def resolved_workflows_path(self) -> Path:
        if self.workflows_path:
            return self.workflows_path
        return self.workflows_path_prod if self.is_galaxy_eu else self.workflows_path_dev


class SupabaseConfig(BaseSettings):
    """Supabase database configuration."""

    url: str = Field(default="", description="Supabase project URL")
    key: str = Field(default="", description="Supabase anon/public key")
    service_key: Optional[str] = Field(default=None, description="Supabase service role key (for admin operations)")
    processor_user_id: Optional[str] = Field(
        default=None,
        description="Supabase auth.users.id for the processor account. Preferred over email-based processor checks.",
    )
    email: Optional[str] = Field(default=None, description="Supabase user email (optional with service_role key)")
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

    access_key: Optional[str] = Field(default=None, description="Legacy storage access key")
    secret_key: Optional[str] = Field(default=None, description="Legacy storage secret key")
    access_key_processor: Optional[str] = Field(default=None, description="Processor access key")
    secret_key_processor: Optional[str] = Field(default=None, description="Processor secret key")
    access_key_uploader: Optional[str] = Field(default=None, description="Uploader access key")
    secret_key_uploader: Optional[str] = Field(default=None, description="Uploader secret key")
    bucket_name: str = Field(default="3dtrees-dev", description="Legacy single bucket name")
    bucket_name_products: str = Field(default="3dtrees-products", description="Products bucket name")
    bucket_name_raw: str = Field(default="3dtrees-raw", description="Raw data bucket name")
    bucket_name_visualization: str = Field(default="3dtrees-visualization", description="Visualization bucket name")
    bucket_name_download: str = Field(default="3dtrees-downloads", description="Private download bucket name")
    url: str = Field(default="http://localhost:9500", description="Storage endpoint URL")
    region: str = Field(default="us-east-1", description="Storage region")

    model_config = SettingsConfigDict(
        case_sensitive=False,
        cli_parse_args=True,
        cli_ignore_unknown_args=True,
        env_prefix="STORAGE_",
        extra="ignore",
    )

    @property
    def processor_access_key(self) -> str:
        return self.access_key_processor or self.access_key or ""

    @property
    def processor_secret_key(self) -> str:
        return self.secret_key_processor or self.secret_key or ""

    @property
    def uploader_access_key(self) -> str:
        return self.access_key_uploader or self.access_key or ""

    @property
    def uploader_secret_key(self) -> str:
        return self.secret_key_uploader or self.secret_key or ""

    @property
    def public_endpoint(self) -> str:
        return self.url

    @property
    def products_bucket(self) -> str:
        return self.bucket_name_products

    @property
    def download_bucket(self) -> str:
        return self.bucket_name_download


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


class LinearConfig(BaseSettings):
    """Linear API configuration for automated bug reporting."""

    api_key: Optional[str] = Field(default=None, description="Linear API key")
    team_id: str = Field(
        default="7ac53333-6ade-4845-b5f5-76ead398222d",
        description="Linear team ID (3DTrees)",
    )
    enabled: bool = Field(
        default=False,
        description="Enable automated Linear issue creation (set to true in production)",
    )

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_prefix="LINEAR_",
        extra="ignore",
    )

    def is_configured(self) -> bool:
        return bool(self.api_key and self.enabled)


class AppConfig:
    """Aggregated configuration with validation on startup."""

    def __init__(self):
        self.galaxy = GalaxyConfig()
        self.supabase = SupabaseConfig()
        self.storage = StorageConfig()
        self.api = APIConfig()

    def validate(self) -> None:
        errors = []

        if not self.supabase.url:
            errors.append("SUPABASE_URL is required")
        if not self.supabase.key:
            errors.append("SUPABASE_KEY is required")

        has_legacy = self.storage.access_key and self.storage.secret_key
        has_processor = self.storage.access_key_processor and self.storage.secret_key_processor
        has_uploader = self.storage.access_key_uploader and self.storage.secret_key_uploader
        if not has_legacy and not (has_processor and has_uploader):
            errors.append(
                "Storage credentials required: either STORAGE_ACCESS_KEY/STORAGE_SECRET_KEY, "
                "or both STORAGE_ACCESS_KEY_PROCESSOR/STORAGE_SECRET_KEY_PROCESSOR and "
                "STORAGE_ACCESS_KEY_UPLOADER/STORAGE_SECRET_KEY_UPLOADER"
            )

        if not self.galaxy.api_key and (not self.galaxy.email or not self.galaxy.password):
            errors.append("Either GALAXY_API_KEY or both GALAXY_EMAIL and GALAXY_PASSWORD are required")

        if errors:
            raise ValueError(f"Configuration errors: {'; '.join(errors)}")
