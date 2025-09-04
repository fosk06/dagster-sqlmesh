#!/usr/bin/env python3
"""
Test script to validate dry-run functionality in a realistic scenario.

This script tests the EnhancedContext dry-run capabilities by:
1. Running a dry-run to see what models would be executed
2. Running an actual sqlmesh run
3. Running another dry-run to verify consistency
"""

import os
import sys
import subprocess
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from dg_sqlmesh.resource import SQLMeshResource


def run_sqlmesh_command(cmd: str, cwd: str = None) -> bool:
    """Run a SQLMesh command and return success status."""
    try:
        result = subprocess.run(
            f"uv run sqlmesh {cmd}",
            shell=True,
            cwd=cwd,
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print(f"❌ Command failed: sqlmesh {cmd}")
            print(f"Error: {result.stderr}")
            return False
        print(f"✅ Command succeeded: sqlmesh {cmd}")
        if result.stdout.strip():
            print(f"Output: {result.stdout.strip()}")
        return True
    except Exception as e:
        print(f"❌ Exception running sqlmesh {cmd}: {e}")
        return False


def test_dry_run_scenario():
    """Test the dry-run functionality in a realistic scenario."""
    print("🧪 Testing EnhancedContext dry-run functionality...")
    
    # Change to the test project directory
    test_project_dir = "tests/fixtures/sqlmesh_project"
    if not os.path.exists(test_project_dir):
        print(f"❌ Test project directory not found: {test_project_dir}")
        return False
    
    os.chdir(test_project_dir)
    print(f"📁 Changed to directory: {os.getcwd()}")
    
    # Initialize SQLMesh resource
    try:
        sqlmesh_resource = SQLMeshResource(
            project_dir=".",
            gateway="duckdb",
            environment="dev"
        )
        print("✅ SQLMesh resource initialized")
    except Exception as e:
        print(f"❌ Failed to initialize SQLMesh resource: {e}")
        return False
    
    # Test 1: Initial dry-run
    print("\n🔍 Test 1: Initial dry-run")
    try:
        completion_status, dry_run_summary = sqlmesh_resource.context.dry_run(
            environment="dev",
            ignore_cron=False  # Don't ignore cron to test realistic scenario
        )
        
        print(f"📊 Initial dry-run results:")
        print(f"  - Completion status: {completion_status}")
        print(f"  - Would execute: {dry_run_summary['would_execute']} models")
        print(f"  - Models to execute: {dry_run_summary.get('successful_models', [])}")
        
    except Exception as e:
        print(f"❌ Initial dry-run failed: {e}")
        return False
    
    # Test 2: Run actual SQLMesh execution
    print("\n🚀 Test 2: Running actual SQLMesh execution")
    if not run_sqlmesh_command("run --environment dev"):
        print("❌ SQLMesh run failed, but continuing with dry-run test")
    
    # Test 3: Dry-run after execution
    print("\n🔍 Test 3: Dry-run after execution")
    try:
        completion_status_after, dry_run_summary_after = sqlmesh_resource.context.dry_run(
            environment="dev",
            ignore_cron=False
        )
        
        print(f"📊 Dry-run after execution results:")
        print(f"  - Completion status: {completion_status_after}")
        print(f"  - Would execute: {dry_run_summary_after['would_execute']} models")
        print(f"  - Models to execute: {dry_run_summary_after.get('successful_models', [])}")
        
        # Compare results
        print(f"\n📈 Comparison:")
        print(f"  - Before execution: {dry_run_summary['would_execute']} models")
        print(f"  - After execution: {dry_run_summary_after['would_execute']} models")
        
        if dry_run_summary['would_execute'] > dry_run_summary_after['would_execute']:
            print("✅ Expected: fewer models after execution (some were processed)")
        elif dry_run_summary['would_execute'] == dry_run_summary_after['would_execute']:
            print("ℹ️  Same number of models (might be due to cron schedules or no changes)")
        else:
            print("⚠️  More models after execution (unexpected)")
            
    except Exception as e:
        print(f"❌ Dry-run after execution failed: {e}")
        return False
    
    # Test 4: Test utility functions
    print("\n🔧 Test 4: Testing utility functions")
    try:
        from dg_sqlmesh.sqlmesh_schedule_utils import should_skip_sqlmesh_run, get_sqlmesh_dry_run_summary
        
        # Test should_skip_sqlmesh_run
        skip_reason = should_skip_sqlmesh_run(
            sqlmesh_resource=sqlmesh_resource,
            context=None,  # Mock context
            environment="dev"
        )
        
        if skip_reason:
            print(f"⏭️  Skip reason: {skip_reason}")
        else:
            print("✅ No skip reason - run should proceed")
        
        # Test get_sqlmesh_dry_run_summary
        status, summary = get_sqlmesh_dry_run_summary(
            sqlmesh_resource=sqlmesh_resource,
            environment="dev"
        )
        
        print(f"📊 Utility summary: {summary['would_execute']} models would execute")
        
    except Exception as e:
        print(f"❌ Utility functions test failed: {e}")
        return False
    
    print("\n🎉 All tests completed!")
    return True


if __name__ == "__main__":
    success = test_dry_run_scenario()
    sys.exit(0 if success else 1)
