import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic_settings import BaseSettings, SettingsConfigDict

from trees_api.connection_manager import ConnectionManager
from trees_api.core.config import AppConfig
from trees_api.galaxy_client import GalaxyClient
from trees_api.integrations.storage.client import StorageClient
from trees_api.integrations.supabase.client import SupabaseClient
from trees_api.integrations.supabase.log_handler import setup_supabase_logging
from trees_api.routes.downloads.router import router as download_router
from trees_api.routes.jobs.router import router as jobs_router
from trees_api.routes.uploads.router import router as upload_router

logger = logging.getLogger("uvicorn")

APP_START_TIME = datetime.now(timezone.utc)


def _get_env_first(*names: str) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _get_package_version() -> Optional[str]:
    try:
        from importlib import metadata
    except Exception:
        return None
    try:
        return metadata.version("3dtrees-api")
    except Exception:
        return None


connection_manager: Optional[ConnectionManager] = None


def get_galaxy_client() -> Optional[GalaxyClient]:
    return connection_manager.get_galaxy_client() if connection_manager else None


def get_supabase_client() -> Optional[SupabaseClient]:
    return connection_manager.get_supabase_client() if connection_manager else None


def get_storage_client() -> Optional[StorageClient]:
    return connection_manager.get_storage_client() if connection_manager else None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global connection_manager

    logger.info("Starting up 3DTrees API...")

    try:
        config = AppConfig()
        config.validate()
        logger.info("Configuration validated successfully")
    except ValueError as error:
        logger.error("Configuration validation failed: %s", error)
        raise RuntimeError(f"Configuration error: {error}") from error

    connection_manager = ConnectionManager(config)

    connection_manager.connect_galaxy()
    connection_manager.connect_supabase()
    connection_manager.connect_storage()
    connection_manager.connect_uploader_storage()

    supabase_client = connection_manager.get_supabase_client()
    if supabase_client:
        setup_supabase_logging(supabase_client.client, source="api")
        logger.info("Supabase logging enabled")

    await connection_manager.start_retry_task(interval=60)

    if connection_manager.all_connected():
        logger.info("All clients initialized successfully")
    else:
        logger.warning(
            "Some clients failed to initialize. API is running in degraded mode."
        )

    yield

    logger.info("Shutting down 3DTrees API...")
    await connection_manager.stop_retry_task()
    connection_manager.cleanup()
    logger.info("Shutdown complete")


app = FastAPI(title="3DTrees API", description="API for 3DTrees", lifespan=lifespan)

ALLOWED_ORIGINS = [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://localhost:4173",
    "http://127.0.0.1:5174",
    "https://3dtrees.earth",
    "https://www.3dtrees.earth",
    "https://threedtrees-dev.web.app",
    "https://threedtrees-dev.firebaseapp.com",
]
ALLOWED_ORIGIN_REGEX = r"https://threedtrees-dev--.*\.web\.app"

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=ALLOWED_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

app.include_router(upload_router)
app.include_router(download_router)
app.include_router(jobs_router)


@app.get("/")
def info():
    status = connection_manager.get_status() if connection_manager else {}
    all_connected = connection_manager.all_connected() if connection_manager else False

    return {
        "message": "3DTrees API is running",
        "status": "healthy" if all_connected else "degraded",
        "services": {
            "galaxy": "connected"
            if status.get("galaxy", {}).get("connected")
            else "disconnected",
            "supabase": "connected"
            if status.get("supabase", {}).get("connected")
            else "disconnected",
            "storage": "connected"
            if status.get("storage", {}).get("connected")
            else "disconnected",
        },
    }


@app.get("/health")
def health_check():
    status = connection_manager.get_status() if connection_manager else {}
    all_connected = connection_manager.all_connected() if connection_manager else False

    return {
        "status": "healthy" if all_connected else "degraded",
        "services": status,
        "message": "All services operational"
        if all_connected
        else "Some services are unavailable",
    }


@app.get("/version")
def version_info():
    git_sha = _get_env_first(
        "GIT_SHA", "GIT_COMMIT", "COMMIT_SHA", "SOURCE_VERSION", "BUILD_SHA"
    )
    build_time = _get_env_first("BUILD_TIME", "BUILD_DATE", "SOURCE_DATE_EPOCH")
    image_tag = _get_env_first("IMAGE_TAG", "DOCKER_IMAGE_TAG", "RELEASE_TAG")
    deploy_env = _get_env_first("DEPLOY_ENV", "ENVIRONMENT", "APP_ENV")

    uptime_seconds = int((datetime.now(timezone.utc) - APP_START_TIME).total_seconds())

    workflows_path = None
    workflow_registry = None
    if connection_manager:
        galaxy_client = connection_manager.get_galaxy_client()
        if galaxy_client:
            workflows_path = str(galaxy_client.workflows_path)
            registry = galaxy_client.get_available_workflow_files()
            workflow_registry = {
                "count": len(registry),
                "workflows": sorted(registry.keys()),
            }

    return {
        "api_version": _get_env_first("API_VERSION", "APP_VERSION")
        or _get_package_version(),
        "git_sha": git_sha,
        "git_sha_short": git_sha[:7] if git_sha else None,
        "build_time": build_time,
        "image_tag": image_tag,
        "environment": deploy_env,
        "python_version": sys.version.split()[0],
        "started_at": APP_START_TIME.isoformat(),
        "uptime_seconds": uptime_seconds,
        "workflows_path": workflows_path,
        "workflow_registry": workflow_registry,
    }


class APIServerSettings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000
    reload: bool = False

    model_config = SettingsConfigDict(
        case_sensitive=False,
        cli_parse_args=True,
        cli_ignore_unknown_args=True,
        env_prefix="API_SERVER_",
    )


def main() -> None:
    import uvicorn

    settings = APIServerSettings()
    uvicorn.run(
        "trees_api.server:app",
        host=settings.host,
        port=settings.port,
        reload=settings.reload,
    )


if __name__ == "__main__":
    main()

