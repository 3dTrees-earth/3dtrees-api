# Integration Tests

This directory contains integration tests for the 3DTrees API, Galaxy workflows, and storage systems.

## Test Files

| File | Description | Duration |
|------|-------------|----------|
| `test_endtoend_workflow.py` | Complete EndToEndPipeline test (GPU required) | ~10-15 min |
| `test_py3dtiles_workflow.py` | Py3DTiles single/multi-file tests | ~2-5 min |
| `test_integration.py` | API health and basic status sync | ~1 min |
| `test_upload.py` | Multipart upload API tests | ~30 sec |
| `conftest.py` | Test fixtures for Galaxy, Supabase, Storage | - |

## Running Tests

### Prerequisites

```bash
make dev           # Start all services (Galaxy, MinIO, Supabase, API)
make galaxy-up     # Or just Galaxy if already have other services
```

### Run All Tests

```bash
make test          # Run all tests
```

### Run Specific Tests

```bash
# EndToEnd pipeline (requires GPU)
make test-endtoend-workflow

# Py3DTiles only
docker compose run --rm api python -m pytest tests/test_py3dtiles_workflow.py -v

# Upload API tests (fast)
docker compose run --rm api python -m pytest tests/test_upload.py -v

# Basic integration tests
docker compose run --rm api python -m pytest tests/test_integration.py -v
```

## Test Configuration

### Two-Bucket Storage System

Tests use a two-bucket system matching production:
- **RAW bucket** (`3dtrees-raw`): Stores raw input data
- **PRODUCTS bucket** (`3dtrees-products`): Stores processed outputs

### Test Data

- `test_data/mikro.laz` - Small point cloud for fast testing
- `test_data/multi-file/` - Multiple tiles for collection tests

## Troubleshooting

**Galaxy authentication fails:**
- Ensure Galaxy user `processor@3dtrees.earth` is registered via the web UI
- Verify Galaxy is running: `curl http://localhost:9090/api/version`

**Supabase connection fails:**
- Check Supabase is running: `cd database && npx supabase status`

**Storage/MinIO issues:**
- Verify MinIO is running: `curl http://localhost:9500/minio/health/live`
- Check buckets exist: Access http://localhost:9501 (minioadmin/minioadmin)

**GPU required tests fail:**
- EndToEnd pipeline requires NVIDIA GPU with CUDA
- SegmentAnyTree step will fail without GPU
