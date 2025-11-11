"""
Test Py3DTiles tool directly without full workflow
Fast isolated test (~1-2 minutes)
"""
import logging
import time
from pathlib import Path

import pytest

from trees_api.models import Dataset
from trees_api.galaxy_client import GalaxyClient

logger = logging.getLogger(__name__)


def test_py3dtiles_direct(galaxy_client: GalaxyClient):
    """
    Test Py3DTiles tool directly by uploading a file and running the tool
    
    This bypasses the full workflow and tests just the py3dtiles conversion
    Much faster than waiting for the entire segmentation workflow!
    """
    logger.info("🧪 Testing Py3DTiles tool directly (no workflow)")
    
    # Create a history
    logger.info("Creating history...")
    history = galaxy_client.gi.histories.create(name="Test - Py3DTiles Direct")
    logger.info(f"✅ Created history: {history.id}")
    
    # Upload the test file
    test_file_path = Path(__file__).parent / "Example_Platane.laz"
    if not test_file_path.exists():
        pytest.skip(f"Test file not found: {test_file_path}")
    
    logger.info(f"Uploading file: {test_file_path.name}")
    dataset = history.upload_file(str(test_file_path))
    
    # Wait for upload
    for i in range(30):
        dataset = dataset.refresh()
        if dataset.state == 'ok':
            break
        time.sleep(1)
    
    assert dataset.state == 'ok', f"Upload failed: {dataset.state}"
    logger.info(f"✅ File uploaded: {dataset.id}")
    
    # Run py3dtiles tool using low-level API
    logger.info("🔧 Running Py3DTiles tool...")
    tool_inputs = {
        'input': {'id': dataset.id, 'src': 'hda'},
        'srs_out': '4978',  # ECEF for Cesium
        'extra_fields': '',  # No extra fields (just testing basic conversion)
        'overwrite': False
    }
    
    tool_output = galaxy_client.gi.gi.tools.run_tool(
        history_id=history.id,
        tool_id='3dtrees_py3dtiles',
        tool_inputs=tool_inputs
    )
    
    job_id = tool_output['jobs'][0]['id']
    logger.info(f"Job submitted: {job_id}")
    
    # Monitor job (max 3 minutes for py3dtiles)
    max_wait = 180
    elapsed = 0
    
    while elapsed < max_wait:
        job = galaxy_client.gi.jobs.get(job_id)
        job_state = job.state
        
        if elapsed % 10 == 0:  # Log every 10 seconds
            logger.info(f"⏳ Job status: {job_state} ({elapsed}/{max_wait}s)")
        
        if job_state in ['ok', 'error', 'failed']:
            break
            
        time.sleep(5)
        elapsed += 5
    
    # Check final status
    job = galaxy_client.gi.jobs.get(job_id)
    logger.info(f"📊 Final job state: {job.state}")
    
    if job.state == 'error':
        stderr = str(getattr(job, 'stderr', '')) if hasattr(job, 'stderr') else ''
        stdout = str(getattr(job, 'stdout', '')) if hasattr(job, 'stdout') else ''
        tool_stderr = str(getattr(job, 'tool_stderr', '')) if hasattr(job, 'tool_stderr') else ''
        
        logger.error(f"❌ Py3DTiles failed!")
        logger.error(f"STDERR ({len(stderr)} chars): {stderr[:500]}")
        logger.error(f"STDOUT ({len(stdout)} chars): {stdout[:500]}")
        logger.error(f"TOOL_STDERR ({len(tool_stderr)} chars): {tool_stderr[:500]}")
        
        raise AssertionError(f"Py3DTiles tool failed: {stderr[:200] or stdout[:200]}")
    
    assert job.state == 'ok', f"Job did not complete successfully: {job.state}"
    logger.info("✅ Py3DTiles tool test PASSED!")
    
    # Verify outputs exist
    outputs = tool_output['outputs']
    logger.info(f"📦 Tool produced {len(outputs)} outputs")
    for output in outputs:
        logger.info(f"  - {output['name']}: {output.get('id')}")

