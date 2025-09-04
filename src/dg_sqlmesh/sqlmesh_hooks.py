"""
SQLMesh Hooks System - Unified dry-run management for SQLMesh operations.

This module provides hooks that:
1. Manage shared dry-run state across all assets in a run
2. Ensure consistent dry-run results across all assets
3. Handle both schedule and manual materialization scenarios
"""

from dagster import (
    success_hook, 
    failure_hook,
    HookContext, 
    ConfigurableResource,
)
from typing import Dict, Any, Optional
import datetime


class SQLMeshSharedStateResource(ConfigurableResource):
    """
    Resource to manage shared state across all SQLMesh assets in a run.
    
    This resource ensures that:
    - Dry-run is performed once per run
    - Results are shared across all assets
    - Execution state is tracked consistently
    """
    
    def __init__(self):
        super().__init__()
        self._dry_run_performed = False
        self._dry_run_results = None
        self._execution_metrics = {
            "assets_processed": 0,
            "assets_skipped": 0,
            "assets_failed": 0,
            "models_executed": 0,
            "models_skipped": 0,
        }
        self._run_start_time = None
        self._run_end_time = None
    
    def perform_shared_dry_run(self, sqlmesh_resource, context, selected_asset_keys):
        """
        Perform dry-run once per run and share results across all assets.
        
        Returns:
            Dict with dry-run results or None if already performed
        """
        if self._dry_run_performed:
            return self._dry_run_results
        
        # Mark as performed
        self._dry_run_performed = True
        self._run_start_time = datetime.datetime.now()
        
        # Perform dry-run using our unified logic
        from .sqlmesh_asset_execution_utils import should_skip_materialization_based_on_dry_run
        
        dry_run_result = should_skip_materialization_based_on_dry_run(
            context, sqlmesh_resource, selected_asset_keys
        )
        
        self._dry_run_results = dry_run_result
        
        # Log dry-run results
        if dry_run_result is None:
            context.log.info("🔍 Shared dry-run: No models to execute")
            self._execution_metrics["models_skipped"] = len(selected_asset_keys)
        else:
            models_to_execute = len(dry_run_result["models_to_materialize"])
            context.log.info(f"🔍 Shared dry-run: {models_to_execute} models will be executed")
            self._execution_metrics["models_executed"] = models_to_execute
        
        return dry_run_result
    
    def record_asset_processed(self, asset_key: str, status: str):
        """Record asset processing status."""
        if status == "success":
            self._execution_metrics["assets_processed"] += 1
        elif status == "skipped":
            self._execution_metrics["assets_skipped"] += 1
        elif status == "failed":
            self._execution_metrics["assets_failed"] += 1
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """Get execution summary for the run."""
        return {
            "dry_run_performed": self._dry_run_performed,
            "dry_run_results": self._dry_run_results,
            "metrics": self._execution_metrics.copy(),
            "run_start_time": self._run_start_time,
            "run_end_time": self._run_end_time,
            "duration": (self._run_end_time - self._run_start_time) if self._run_end_time else None
        }


@success_hook(required_resource_keys={"sqlmesh_shared_state"})
def sqlmesh_success_hook(context: HookContext):
    """Hook for successful SQLMesh asset materialization."""
    asset_key = context.op.name
    shared_state = context.resources.sqlmesh_shared_state
    
    # Record success
    shared_state.record_asset_processed(asset_key, "success")
    
    context.log.info(f"✅ SQLMesh success hook: {asset_key}")
    
    return None


@failure_hook(required_resource_keys={"sqlmesh_shared_state"})
def sqlmesh_failure_hook(context: HookContext):
    """Hook for failed SQLMesh asset materialization."""
    asset_key = context.op.name
    error = context.step.failure_data.error if context.step.failure_data else "Unknown error"
    shared_state = context.resources.sqlmesh_shared_state
    
    # Record failure
    shared_state.record_asset_processed(asset_key, "failed")
    
    context.log.error(f"❌ SQLMesh failure hook: {asset_key} - {error}")
    
    return None


def get_sqlmesh_hooks():
    """
    Get the complete set of SQLMesh hooks for use in jobs.
    
    Returns:
        Set of hooks to be used in job configuration
    """
    return {
        sqlmesh_success_hook,
        sqlmesh_failure_hook
    }


def get_sqlmesh_resources():
    """
    Get the complete set of SQLMesh resources for use in definitions.
    
    Returns:
        Dict of resources to be used in definitions
    """
    return {
        "sqlmesh_shared_state": SQLMeshSharedStateResource(),
    }
