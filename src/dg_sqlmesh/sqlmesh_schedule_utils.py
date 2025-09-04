"""
Utilities for SQLMesh schedules.
"""

import datetime
from typing import Optional, Tuple, Dict, Any
from dagster import SkipReason, ScheduleEvaluationContext
from sqlmesh.utils import CompletionStatus
from .resource import SQLMeshResource


def should_skip_sqlmesh_run(
    sqlmesh_resource: SQLMeshResource,
    context: ScheduleEvaluationContext,
    environment: Optional[str] = None,
) -> Optional[SkipReason]:
    """
    Determines if a SQLMesh run should be skipped based on a dry-run.

    Args:
        sqlmesh_resource: The configured SQLMesh resource
        context: The Dagster schedule evaluation context
        environment: The SQLMesh environment to use (optional)

    Returns:
        SkipReason if the run should be skipped, None otherwise
    """
    try:
        # Create a temporary SQLMesh resource for dry-run
        temp_sqlmesh_resource = SQLMeshResource(
            project_dir=sqlmesh_resource.project_dir,
            gateway=sqlmesh_resource.gateway,
            environment=sqlmesh_resource.environment,
            translator=sqlmesh_resource.translator,
            concurrency_limit=sqlmesh_resource.concurrency_limit,
        )

        # Perform dry-run to check if there are models to execute
        completion_status, dry_run_summary = temp_sqlmesh_resource.context.dry_run(
            environment=environment or sqlmesh_resource.environment,
            execution_time=context.scheduled_execution_time or datetime.datetime.now(),
        )

        # Check if there are models to execute
        if completion_status.is_nothing_to_do or dry_run_summary["would_execute"] == 0:
            return SkipReason(
                f"No new data available - nothing to process. Dry-run summary: {dry_run_summary['would_execute']} models would be executed"
            )

        context.log.info(
            f"SQLMesh dry-run completed: {dry_run_summary['would_execute']} models will be executed"
        )

        return None  # No skip, continue with the run

    except Exception as e:
        context.log.warning(f"SQLMesh dry-run failed, proceeding with run: {e}")
        return None  # In case of error, continue with the run as fallback


def get_sqlmesh_dry_run_summary(
    sqlmesh_resource: SQLMeshResource,
    environment: Optional[str] = None,
    execution_time: Optional[datetime.datetime] = None,
) -> Tuple[CompletionStatus, Dict[str, Any]]:
    """
    Gets a summary of the SQLMesh dry-run.

    Args:
        sqlmesh_resource: The configured SQLMesh resource
        environment: The SQLMesh environment to use (optional)
        execution_time: The execution time (optional)

    Returns:
        Tuple[CompletionStatus, Dict]: (status, dry_run_summary)
    """
    # Create a temporary SQLMesh resource for dry-run
    temp_sqlmesh_resource = SQLMeshResource(
        project_dir=sqlmesh_resource.project_dir,
        gateway=sqlmesh_resource.gateway,
        environment=sqlmesh_resource.environment,
        translator=sqlmesh_resource.translator,
        concurrency_limit=sqlmesh_resource.concurrency_limit,
    )

    # Perform the dry-run
    completion_status, dry_run_summary = temp_sqlmesh_resource.context.dry_run(
        environment=environment or sqlmesh_resource.environment,
        execution_time=execution_time or datetime.datetime.now(),
    )

    return completion_status, dry_run_summary
