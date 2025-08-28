from __future__ import annotations

from typing import Any, List
from dagster import AssetExecutionContext, AssetKey

from .resource import SQLMeshResource


def _log_run_selection(
    context: AssetExecutionContext, run_id: str, selected_asset_keys: List[AssetKey]
) -> None:
    """Log high-level context for the shared execution."""
    context.log.info(
        "First asset in run; launching SQLMesh execution for all selected assets"
    )
    context.log.debug(f"No existing results for run {run_id}")
    context.log.info(f"Selected assets in this run: {selected_asset_keys}")


def _materialize_and_get_plan(
    sqlmesh: SQLMeshResource,
    models_to_materialize: List[Any],
    context: AssetExecutionContext,
) -> Any:
    """Run a single SQLMesh materialization and return the plan."""
    context.log.info(
        f"Materializing {len(models_to_materialize)} models: {[m.name for m in models_to_materialize]}"
    )
    context.log.debug(
        "Starting SQLMesh materialization (count=%d)", len(models_to_materialize)
    )
    plan = sqlmesh.materialize_assets_threaded(models_to_materialize, context=context)
    context.log.debug("SQLMesh materialization completed")
    return plan
