#!/usr/bin/env python3
"""
Test script for Linear client integration.

Run from the 3dtrees-api directory:
    python tests/test_linear_client.py
"""
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables from .env file
from dotenv import load_dotenv
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
load_dotenv(env_path)

from trees_api.config import LinearConfig
from trees_api.linear_client import LinearClient, FailedJob, create_linear_client_if_enabled


def test_config():
    """Test Linear configuration loading."""
    print("=" * 60)
    print("LINEAR CONFIGURATION TEST")
    print("=" * 60)
    
    config = LinearConfig()
    print(f"API Key set: {bool(config.api_key)}")
    print(f"API Key preview: {config.api_key[:10]}..." if config.api_key else "API Key: None")
    print(f"Team ID: {config.team_id}")
    print(f"Enabled: {config.enabled}")
    print(f"Is configured: {config.is_configured()}")
    print()
    
    return config


def test_client_creation():
    """Test Linear client creation."""
    print("=" * 60)
    print("LINEAR CLIENT CREATION TEST")
    print("=" * 60)
    
    client = create_linear_client_if_enabled()
    
    if client:
        print("✅ Linear client created successfully")
        print(f"   Enabled: {client.enabled}")
        return client
    else:
        print("❌ Linear client not created (disabled or not configured)")
        return None


def test_duplicate_check(client: LinearClient):
    """Test duplicate issue detection."""
    print("=" * 60)
    print("DUPLICATE CHECK TEST")
    print("=" * 60)
    
    # Test with a fake invocation ID that shouldn't exist
    test_invocation_id = "test-fake-invocation-12345"
    existing = client._check_existing_issue(test_invocation_id)
    
    if existing:
        print(f"Found existing issue: {existing}")
    else:
        print("✅ No duplicate found (expected)")
    print()


def test_create_issue(client: LinearClient, dry_run: bool = True):
    """Test issue creation (dry run by default)."""
    print("=" * 60)
    print(f"ISSUE CREATION TEST {'(DRY RUN)' if dry_run else '(LIVE)'}")
    print("=" * 60)
    
    # Create test data
    failed_jobs = [
        FailedJob(
            tool_id="toolshed.g2.bx.psu.edu/repos/bgruening/3dtrees_standardization/3dtrees_standardization/1.1.0+galaxy0",
            tool_name="3dtrees_standardization",
            exit_code=1,
            stderr="Test error: This is a test stderr message from the Linear integration test.",
            stdout="Test stdout output",
            job_messages=["Test job message 1", "Test job message 2"],
        ),
    ]
    
    messages = [
        {"message": "Test workflow message", "type": "info"},
    ]
    
    print("Test data:")
    print(f"  Dataset ID: 999999 (test)")
    print(f"  Invocation ID: test-linear-integration-{os.getpid()}")
    print(f"  Failed jobs: {len(failed_jobs)}")
    print()
    
    if dry_run:
        print("🔸 Dry run mode - not creating actual issue")
        print("  To create a real test issue, run with --live flag")
        
        # Show what the description would look like
        description = client._build_issue_description(
            dataset_id=999999,
            invocation_id=f"test-linear-integration-{os.getpid()}",
            failed_jobs=failed_jobs,
            messages=messages,
        )
        print()
        print("Issue description preview:")
        print("-" * 40)
        print(description[:500] + "..." if len(description) > 500 else description)
        print("-" * 40)
    else:
        print("🔴 LIVE mode - creating actual issue...")
        issue_id = client.create_workflow_failure_issue(
            dataset_id=999999,
            invocation_id=f"test-linear-integration-{os.getpid()}",
            workflow_name="TestWorkflow",
            failed_jobs=failed_jobs,
            messages=messages,
        )
        
        if issue_id:
            print(f"✅ Created issue: {issue_id}")
            print(f"   URL: https://linear.app/geosense-ufr/issue/{issue_id}")
        else:
            print("❌ Failed to create issue")
    
    print()


def main():
    """Run all tests."""
    live_mode = "--live" in sys.argv
    
    print()
    print("🧪 LINEAR CLIENT INTEGRATION TEST")
    print("=" * 60)
    print()
    
    # Test config
    config = test_config()
    
    if not config.is_configured():
        print("⚠️  Linear is not configured. Set these environment variables:")
        print("    LINEAR_API_KEY=<your-api-key>")
        print("    LINEAR_ENABLED=true")
        print()
        print("You can also add them to .env file")
        return 1
    
    # Test client creation
    client = test_client_creation()
    if not client:
        return 1
    
    print()
    
    # Test duplicate check
    test_duplicate_check(client)
    
    # Test issue creation
    test_create_issue(client, dry_run=not live_mode)
    
    print("=" * 60)
    print("✅ All tests completed")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
