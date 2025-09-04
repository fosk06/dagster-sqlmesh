#!/usr/bin/env python3
"""
Simple test script to validate EnhancedContext functionality.

This script tests the basic EnhancedContext capabilities:
1. Method delegation to SQLMesh Context
2. Dry-run functionality
3. Utility methods
"""

import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from dg_sqlmesh.resource import SQLMeshResource
from dg_sqlmesh.enhanced_context import EnhancedContext


def test_enhanced_context_basic():
    """Test basic EnhancedContext functionality."""
    print("🧪 Testing EnhancedContext basic functionality...")
    
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
    
    # Get the enhanced context
    enhanced_context = sqlmesh_resource.context
    print(f"✅ EnhancedContext type: {type(enhanced_context)}")
    
    # Test 1: Method delegation
    print("\n🔍 Test 1: Method delegation")
    try:
        # Test that we can access SQLMesh Context methods
        config = enhanced_context.config
        print(f"✅ Config accessed: {type(config)}")
        
        # Test that we can access environment
        default_env = enhanced_context.config.default_target_environment
        print(f"✅ Default environment: {default_env}")
        
        # Test that we can access snapshots
        snapshots = enhanced_context.snapshots
        print(f"✅ Snapshots accessed: {len(snapshots)} snapshots")
        
    except Exception as e:
        print(f"❌ Method delegation failed: {e}")
        return False
    
    # Test 2: Dry-run functionality
    print("\n🔍 Test 2: Dry-run functionality")
    try:
        completion_status, dry_run_summary = enhanced_context.dry_run(
            environment="dev",
            ignore_cron=True  # Ignore cron for this test
        )
        
        print(f"📊 Dry-run results:")
        print(f"  - Completion status: {completion_status}")
        print(f"  - Would execute: {dry_run_summary['would_execute']} models")
        print(f"  - Total simulated: {dry_run_summary['total_simulated']}")
        print(f"  - Models to execute: {dry_run_summary.get('successful_models', [])}")
        
    except Exception as e:
        print(f"❌ Dry-run failed: {e}")
        return False
    
    # Test 3: Utility methods
    print("\n🔧 Test 3: Utility methods")
    try:
        # Test will_run_execute_models
        will_execute = enhanced_context.will_run_execute_models(
            environment="dev",
            ignore_cron=True
        )
        print(f"✅ will_run_execute_models: {will_execute}")
        
        # Test get_models_to_execute
        models_to_execute = enhanced_context.get_models_to_execute(
            environment="dev",
            ignore_cron=True
        )
        print(f"✅ get_models_to_execute: {models_to_execute}")
        
    except Exception as e:
        print(f"❌ Utility methods failed: {e}")
        return False
    
    # Test 4: Dry-run evaluator
    print("\n🔧 Test 4: Dry-run evaluator")
    try:
        evaluator = enhanced_context.dry_run_evaluator
        print(f"✅ Dry-run evaluator type: {type(evaluator)}")
        
        # Test evaluator methods
        summary = evaluator.get_dry_run_summary()
        print(f"✅ Evaluator summary: {summary}")
        
        # Test clear simulation
        evaluator.clear_simulation()
        print("✅ Simulation cleared")
        
    except Exception as e:
        print(f"❌ Dry-run evaluator failed: {e}")
        return False
    
    print("\n🎉 All EnhancedContext tests completed!")
    return True


if __name__ == "__main__":
    success = test_enhanced_context_basic()
    sys.exit(0 if success else 1)
