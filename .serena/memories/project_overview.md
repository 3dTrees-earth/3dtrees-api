# 3dtrees-api Project Overview

## Purpose
FastAPI backend for the 3DTrees platform — handles dataset processing workflows (Galaxy), file uploads (S3), downloads, and notifications.

## Tech Stack
- Python 3.12+, FastAPI, Pydantic v2, pydantic-settings
- Supabase (auth + database), Galaxy (workflow engine), S3/MinIO (storage)
- httpx for HTTP calls, boto3 for S3
- Black (line-length=88), isort (profile=black), flake8, mypy (strict)

## Project Structure
```
trees_api/
  app/server.py              # FastAPI app, lifespan, router registration
  app/connection_manager.py  # Singleton for managing integration clients
  core/config.py             # Pydantic BaseSettings classes per service
  core/models.py             # Shared Pydantic models
  integrations/              # External service clients (galaxy, supabase, storage, linear, notifications)
  routes/                    # API routers (uploads, downloads, jobs)
  workers/                   # Background workers (status_sync, result_sync)
```

## Key Patterns
- **Config**: Pydantic BaseSettings per service, aggregated in AppConfig, validated at startup
- **Integration clients**: Class with connect() + methods, created by ConnectionManager singleton
- **Routers**: APIRouter with prefix/tags, Pydantic request/response models, Depends() for DI
- **Service layer**: Business logic in service.py, HTTP in router.py (see routes/jobs/)
- **Existing Brevo integration**: `integrations/notifications/client.py` has BrevoEmailConfig + send_email_via_brevo() for transactional emails

## Commands
- Run: `uvicorn trees_api.app.server:app --reload`
- Test: `pytest`
- Format: `black . && isort .`
- Lint: `flake8 && mypy .`
