#!/usr/bin/env python3
"""
Test script to verify the simplified SQLMesh hooks system.
"""

import sys
import os
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from src.dg_sqlmesh.sqlmesh_hooks import (
    SQLMeshSharedStateResource,
    get_sqlmesh_hooks,
    get_sqlmesh_resources
)

def test_hooks_system():
    """Test the simplified SQLMesh hooks system."""
    print("🧪 Testing simplified SQLMesh hooks system...")
    
    # Test resources creation
    print("\n📦 Testing resource creation...")
    
    # Test shared state resource
    shared_state = SQLMeshSharedStateResource()
    print(f"✅ SQLMeshSharedStateResource: {type(shared_state)}")
    print(f"  - Dry run performed: {shared_state._dry_run_performed}")
    print(f"  - Execution metrics: {shared_state._execution_metrics}")
    
    # Test hooks retrieval
    print("\n🔗 Testing hooks retrieval...")
    hooks = get_sqlmesh_hooks()
    print(f"✅ Hooks retrieved: {len(hooks)} hooks")
    for hook in hooks:
        print(f"  - {hook.__name__}")
    
    # Test resources retrieval
    print("\n🔧 Testing resources retrieval...")
    resources = get_sqlmesh_resources()
    print(f"✅ Resources retrieved: {len(resources)} resources")
    for name, resource in resources.items():
        print(f"  - {name}: {type(resource)}")
    
    # Test shared state functionality
    print("\n🔄 Testing shared state functionality...")
    
    # Simulate dry-run
    shared_state._dry_run_performed = True
    shared_state._run_start_time = "2025-09-04 14:30:00"
    shared_state._execution_metrics["assets_processed"] = 5
    shared_state._execution_metrics["assets_skipped"] = 2
    shared_state._execution_metrics["assets_failed"] = 0
    
    # Get execution summary
    summary = shared_state.get_execution_summary()
    print(f"✅ Execution summary: {summary}")
    
    print("\n🎉 All tests completed successfully!")

if __name__ == "__main__":
    test_hooks_system()
