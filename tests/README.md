# Integration Tests

This directory contains integration tests for the 3DTrees API, Galaxy workflows, and storage systems.

## Test Configuration

The tests are designed to match the production setup as closely as possible:

### Two-Bucket Storage System

Tests use a two-bucket system matching production:
- **RAW bucket** (`3dtrees-tool-raw`): Stores raw input data (like `frct-3dtrees-raw-dev` in production)
- **PRODUCTS bucket** (`3dtrees-tool-products`): Stores processed outputs (like `frct-3dtrees-products-dev` in production)

### Running Tests Locally

**1. Start local services:**
```bash
make local-supabase  # Starts local Supabase
make galaxy-up       # Starts Galaxy
```

**2. Register Galaxy user (first time only):**
- Open http://localhost:9090/login/start
- Register with:
  - Email: `processor@3dtrees.earth`
  - Password: `processor`

**3. Create MinIO buckets (for local testing):**
```bash
# Access MinIO console at http://localhost:9501
# Login: minioadmin / minioadmin
# Create buckets:
#   - 3dtrees-tool-raw
#   - 3dtrees-tool-products
```

Or via CLI:
```bash
docker run --rm --network host --entrypoint sh minio/mc -c "\
  mc alias set minio http://localhost:9500 minioadmin minioadmin && \
  mc mb minio/3dtrees-tool-raw && \
  mc mb minio/3dtrees-tool-products"
```

**4. Run tests:**
```bash
# Run all tests
make test

# Run specific test
docker compose run --rm api python -m pytest tests/test_integration.py::test_status_sync_standalone -v

# Run with verbose output
docker compose run --rm api python -m pytest tests/ -v -s
```

### Running Tests with Production-like S3

To test against the University Freiburg S3 (read-only):

**1. Update `.env.test`:**
```bash
# Uncomment production S3 settings:
STORAGE_URL=https://s3.bwsfs.uni-freiburg.de
STORAGE_ACCESS_KEY=<analyst-access-key>
STORAGE_SECRET_KEY=<analyst-secret-key>
STORAGE_REGION=fr1-ec82
STORAGE_BUCKET_NAME_RAW=frct-3dtrees-raw-dev
STORAGE_BUCKET_NAME_PRODUCTS=frct-3dtrees-products-dev
```

**2. Run tests:**
```bash
docker compose run --rm --env-file .env.test api python -m pytest tests/ -v
```

**Note:** When using production S3 with read-only access (analyst profile), some tests may fail if they try to upload files.

## Test Structure

- `conftest.py`: Test fixtures for Galaxy, Supabase, and Storage clients
- `test_integration.py`: End-to-end integration tests
- `test_workflow.py`: Galaxy workflow execution tests

## Current Test Status

✅ **Passing Tests:**
- `test_api_health_check`: Basic API connectivity
- `test_status_sync_standalone`: Supabase and Galaxy status synchronization

⚠️ **Tests requiring MinIO setup:**
- `test_workflow_via_api_with_status_sync`: Full workflow via API
- `test_workflow`: Direct workflow execution

## Troubleshooting

**Galaxy authentication fails:**
- Ensure Galaxy user `processor@3dtrees.earth` is registered via the web UI
- Verify Galaxy is running: `curl http://localhost:9090/api/version`

**Supabase connection fails:**
- Check Supabase is running: `cd database && npx supabase status`
- Verify URL in `.local-supabase.env` uses `localhost` not `host.docker.internal`

**Storage/MinIO issues:**
- Verify MinIO is running: `curl http://localhost:9500/minio/health/live`
- Check buckets exist: Access http://localhost:9501 (minioadmin/minioadmin)
- Ensure both buckets are created: `3dtrees-tool-raw` and `3dtrees-tool-products`

**Network issues:**
- Tests use `network_mode: "host"` in docker-compose.yml
- All services should use `localhost` URLs, not `host.docker.internal`

