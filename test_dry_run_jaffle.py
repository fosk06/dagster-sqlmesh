#!/usr/bin/env python3
"""
Test script to check dry-run functionality in jaffle-platform.
"""

import sys
import os
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from src.jaffle_platform.definitions import defs

def test_dry_run():
    """Test dry-run functionality with jaffle-platform."""
    print("🧪 Testing dry-run functionality in jaffle-platform...")
    
    # Get the SQLMesh resource
    sqlmesh_resource = defs.resources["sqlmesh"]
    
    print(f"✅ SQLMesh resource: {type(sqlmesh_resource)}")
    print(f"📁 Project dir: {sqlmesh_resource.project_dir}")
    print(f"🌐 Gateway: {sqlmesh_resource.gateway}")
    print(f"🏭 Environment: {sqlmesh_resource.environment}")
    
    # Test dry-run
    print("\n🔍 Testing dry-run...")
    try:
        completion_status, dry_run_summary = sqlmesh_resource.context.dry_run(
            environment="dev",
            ignore_cron=False  # Don't ignore cron to test realistic scenario
        )
        
        print(f"📊 Dry-run results:")
        print(f"  - Completion status: {completion_status}")
        print(f"  - Would execute: {dry_run_summary['would_execute']} models")
        print(f"  - Total simulated: {dry_run_summary['total_simulated']}")
        print(f"  - Models to execute: {dry_run_summary.get('successful_models', [])}")
        
        if dry_run_summary['would_execute'] > 0:
            print("✅ Models need execution - dry-run detected work to do!")
        else:
            print("ℹ️  No models need execution - all up to date or cron not ready")
            
    except Exception as e:
        print(f"❌ Dry-run failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_dry_run()
