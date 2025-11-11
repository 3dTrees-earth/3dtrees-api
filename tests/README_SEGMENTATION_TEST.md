# Segmentation Workflow Tests

## Overview

This directory contains tests for the Galaxy workflows, including the new **Segmentation workflow test**.

## Test Files

### `test_workflow.py`
- **Tests**: Overviews workflow  
- **GPU**: Optional (works on CPU)
- **Runtime**: ~19 minutes on CPU
- **Status**: ✅ Fully functional

### `test_segmentation_workflow.py` (NEW)
- **Tests**: Full Segmentation workflow (Standardization → Overviews → SegmentAnyTree → Py3DTiles)
- **GPU**: **REQUIRED** for SegmentAnyTree step
- **Runtime**: Cannot complete without GPU
- **Status**: ⚠️ Will skip with informative message when GPU not available

## Running Tests

### Test Overviews Workflow (Works on CPU)
```bash
cd /home/jj1049/3dtrees
docker compose run --rm api python -m pytest tests/test_workflow.py::test_workflow -v -s
```

### Test Segmentation Workflow (Requires GPU)
```bash
cd /home/jj1049/3dtrees
docker compose run --rm api python -m pytest tests/test_segmentation_workflow.py::test_segmentation_workflow -v -s
```

**Expected behavior without GPU:**
```
SKIPPED [1] SegmentAnyTree requires GPU. Test cannot complete with Planemo (GPU not available).
This is expected - deploy to production Galaxy with GPU support.
```

### Test SegmentAnyTree Tool Directly
```bash
docker compose run --rm api python -m pytest tests/test_segmentation_workflow.py::test_segmentanytree_tool_direct -v -s
```

## What the Tests Do

### `test_segmentation_workflow`
1. ✅ Uploads test LAZ file to Galaxy
2. ✅ Invokes full Segmentation workflow
3. ✅ Monitors all workflow steps:
   - Standard (Standardization)
   - Overviews (3D visualizations)
   - SegmentAnyTree (Deep learning segmentation) - **Requires GPU**
   - Py3DTiles (3D tile generation)
4. ⚠️ Detects GPU-related errors in SegmentAnyTree
5. ✅ Skips test gracefully with informative message
6. ✅ Records workflow invocation in Supabase

### `test_segmentanytree_tool_direct`
- Tests only the SegmentAnyTree tool (not full workflow)
- Useful for debugging segmentation issues
- Also skips gracefully without GPU

## GPU Requirements

### Why GPU is Required

The **SegmentAnyTree** tool uses PyTorch deep learning models that require CUDA:

```python
import torch
assert torch.cuda.is_available(), "SegmentAnyTree requires CUDA GPU"
```

**From the tool's test suite:**
```xml
<test expect_exit_code="1" expect_failure="true">
    <assert_stderr>
        <has_text text="RuntimeError: Found no NVIDIA driver"/>
    </assert_stderr>
</test>
```

The tool is explicitly designed to fail without GPU access.

## Current Limitations

### Planemo GPU Support
**Issue**: Planemo (local Galaxy testing tool) doesn't support `docker_run_extra_arguments` for GPU passthrough.

**Created config**: `galaxy/tools/job_conf_gpu.yml` with:
```yaml
docker_run_extra_arguments: '--gpus "device=3"'
```

**Problem**: Planemo ignores this configuration.

**Verified working**:
```bash
# Direct Docker test with GPU works:
docker run --gpus '"device=3"' ghcr.io/3dtrees-earth/3dtrees_sat:1.1.0 \
    python3.8 -c "import torch; print(torch.cuda.is_available())"
# Output: True ✅
```

## Solutions

### For Local Development
1. **Overviews workflow**: Use CPU-based testing (works fine)
2. **SegmentAnyTree tool**: Test algorithm directly with Docker:
   ```bash
   docker run --gpus '"device=3"' \
       -v $(pwd)/test_data:/data \
       ghcr.io/3dtrees-earth/3dtrees_sat:1.1.0 \
       python3.8 /src/run.py --dataset-path /data/input.laz --output-dir /data
   ```

### For Production/CI
Deploy workflows to:
- Galaxy EU (https://usegalaxy.eu)
- Production Galaxy instance with GPU support
- Custom Galaxy installation (not Planemo) with `job_conf_gpu.yml`

In production Galaxy, the `job_conf_gpu.yml` configuration will work correctly.

## Test Output Examples

### Successful Skip (Expected)
```
tests/test_segmentation_workflow.py::test_segmentation_workflow 
⏳ Monitoring workflow execution...
Attempt 5/180: Workflow status = scheduled, Jobs = 4
  Job states: Standard=ok, Overviews=ok, SegmentAnyTree=error, Py3DTiles=new
⚠️ Expected GPU error in SegmentAnyTree job: RuntimeError: Found no NVIDIA driver
SKIPPED [1] SegmentAnyTree requires GPU...
```

### Successful Completion (With GPU)
```
✅ Workflow invoked successfully: abc123
⏳ Monitoring workflow execution...
✅ All jobs completed. Final status: ok
📦 Workflow outputs: 2 files, 3 collections
✅ Test passed! Segmentation workflow completed successfully with GPU support.
PASSED
```

## Future Work

- [ ] Add mock/stub for GPU testing (if feasible)
- [ ] Create smaller test dataset for faster GPU tests
- [ ] Add performance benchmarks (CPU vs GPU timing)
- [ ] Document production Galaxy deployment with GPU config

## Related Documentation

- `.cursor/galaxy-debugging.md` - Galaxy debugging guide
- `.cursor/segmentation-gpu-status.md` - Detailed GPU analysis
- `.cursor/gpu-status.md` - GPU configuration status
- `galaxy/tools/job_conf_gpu.yml` - GPU job configuration (for production)


