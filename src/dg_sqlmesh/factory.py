from dagster import (
    asset,
    AssetExecutionContext,
    schedule,
    define_asset_job,
    RunRequest,
    Definitions,
    ConfigurableResource,
    RetryPolicy,
    AssetSelection,
    SkipReason,
    RunsFilter,
    DagsterRunStatus,
    DagsterInstance,
    Failure,
)
from dagster._core.run_coordinator import QueuedRunCoordinator
from .resource import SQLMeshResource
from .sqlmesh_asset_utils import (
    get_asset_kinds,
    create_asset_specs,
    get_extra_keys,
    validate_external_dependencies,
)
from .sqlmesh_asset_check_utils import create_asset_checks_from_model
from sqlmesh.core.model.definition import ExternalModel
import datetime
from .translator import SQLMeshTranslator
from typing import Optional, Dict, List, Any
import warnings

# Import utility functions
from .sqlmesh_asset_execution_utils import (
    execute_sqlmesh_materialization,
    process_sqlmesh_results,
    check_model_status,
    create_materialize_result,
)


class SQLMeshResultsResource(ConfigurableResource):
    """Resource to share SQLMesh results between assets within the same run."""

    def __init__(self):
        super().__init__()
        self._results = {}

    def store_results(self, run_id: str, results: Dict[str, Any]) -> None:
        """Store SQLMesh results for a given run."""
        self._results[run_id] = results

    def get_results(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve SQLMesh results for a given run."""
        return self._results.get(run_id)

    def has_results(self, run_id: str) -> bool:
        """Check if results exist for a given run."""
        return run_id in self._results


# -------------------- Concurrency/Instance validation helpers --------------------
_CONCURRENCY_CONFIG_ERROR = (
    "dagster-sqlmesh requires QueuedRunCoordinator for safe SQLMesh scheduling.\n"
    "Configure dagster.yaml:\n"
    "run_coordinator:\n"
    "  module: dagster._core.run_coordinator.queued_run_coordinator\n"
    "  class: QueuedRunCoordinator\n\n"
    "Also ensure concurrency key 'sqlmesh_jobs_exclusive' has limit: 1:\n"
    "concurrency:\n"
    "  - concurrency_key: sqlmesh_jobs_exclusive\n"
    "    limit: 1\n"
)


def _assert_instance_has_queued_run_coordinator(instance) -> None:
    """Raise RuntimeError if the Dagster instance is not using QueuedRunCoordinator."""
    if not isinstance(instance.run_coordinator, QueuedRunCoordinator):
        raise RuntimeError(_CONCURRENCY_CONFIG_ERROR)


def _assert_instance_for_compute() -> None:
    """Fail in compute if instance is missing or not using QueuedRunCoordinator."""
    instance = None
    try:
        instance = DagsterInstance.get()
    except Exception:
        instance = None
    if instance is None or not isinstance(
        instance.run_coordinator, QueuedRunCoordinator
    ):
        raise Failure(
            _CONCURRENCY_CONFIG_ERROR,
            allow_retries=False,
        )


def build_sqlmesh_job(sqlmesh_assets, name: str = "sqlmesh_job"):
    selected_assets = AssetSelection.assets(
        *(key for ad in sqlmesh_assets for key in ad.keys)
    )
    safe_selection = selected_assets.required_multi_asset_neighbors()
    return define_asset_job(
        name=name,
        selection=safe_selection,
        op_retry_policy=RetryPolicy(max_retries=0),
        tags={
            "dagster/max_retries": "0",
            "dagster/retry_on_asset_or_op_failure": "false",
            # Concurrency key to allow instance-level singleton enforcement
            "dagster/concurrency_key": "sqlmesh_jobs_exclusive",
        },
    )


def sqlmesh_assets_factory(
    *,
    sqlmesh_resource: SQLMeshResource,
    group_name: str = "sqlmesh",
    op_tags: Optional[Dict[str, Any]] = None,
    owners: Optional[List[str]] = None,
):
    """
    Factory to create SQLMesh Dagster assets.
    """
    try:
        extra_keys = get_extra_keys()
        kinds = get_asset_kinds(sqlmesh_resource)
        specs = create_asset_specs(
            sqlmesh_resource, extra_keys, kinds, owners, group_name
        )
    except Exception as e:
        raise ValueError(f"Failed to create SQLMesh assets: {e}") from e

    # Create individual assets with shared SQLMesh execution
    assets = []

    def create_model_asset(
        current_model_name, current_asset_spec, current_model_checks
    ):
        @asset(
            key=current_asset_spec.key,
            description=f"SQLMesh model: {current_model_name}",
            group_name=current_asset_spec.group_name,
            metadata=current_asset_spec.metadata,
            deps=current_asset_spec.deps,
            check_specs=current_model_checks,
            op_tags=op_tags,
            retry_policy=RetryPolicy(max_retries=0),
            # Force no retries to prevent infinite loops with SQLMesh audit failures
            tags={
                **(current_asset_spec.tags or {}),
                "sqlmesh": "",  # Tag to identify SQLMesh assets
                "dagster/max_retries": "0",
                "dagster/retry_on_asset_or_op_failure": "false",
            },
        )
        def model_asset(
            context: AssetExecutionContext,
            sqlmesh: SQLMeshResource,
            sqlmesh_results: SQLMeshResultsResource,
        ):
            _assert_instance_for_compute()
            context.log.info(f"Processing SQLMesh model: {current_model_name}")

            # Check if SQLMesh was already executed in this run
            run_id = context.run_id

            # Retrieve or create shared SQLMesh results
            if not sqlmesh_results.has_results(run_id):
                # Execute SQLMesh materialization for all selected assets
                execute_sqlmesh_materialization(
                    context,
                    sqlmesh,
                    sqlmesh_results,
                    run_id,
                    context.selected_asset_keys,
                )

            # Retrieve results for this run
            (
                failed_check_results,
                skipped_models_events,
                non_blocking_audit_warnings,
                notifier_audit_failures,
                affected_downstream_asset_keys,
                sqlmesh_executed_models,
                sqlmesh_skipped_models,
            ) = process_sqlmesh_results(context, sqlmesh_results, run_id)

            # Check the status for our specific model
            model_was_skipped, model_has_audit_failures = check_model_status(
                context,
                current_model_name,
                current_asset_spec,
                failed_check_results,
                skipped_models_events,
            )

            # Create the appropriate MaterializeResult (9-params API)
            result = create_materialize_result(
                context,
                current_model_name,
                current_asset_spec,
                current_model_checks,
                model_was_skipped,
                model_has_audit_failures,
                non_blocking_audit_warnings,
                notifier_audit_failures,
                affected_downstream_asset_keys,
                sqlmesh_executed_models,  # Pass SQLMesh executed models
                sqlmesh_skipped_models,  # Pass SQLMesh skipped models
            )
            return result

        # Rename to avoid collisions
        model_asset.__name__ = f"sqlmesh_{current_model_name}_asset"
        return model_asset

    # Use existing utilities
    models = sqlmesh_resource.get_models()

    # Create assets for each model that has an AssetSpec
    for model in models:
        # Ignore external models
        if isinstance(model, ExternalModel):
            continue

        # Use translator to get the AssetKey
        asset_key = sqlmesh_resource.translator.get_asset_key(model)

        # Find the matching AssetSpec in the list
        asset_spec = None
        for spec in specs:
            if spec.key == asset_key:
                asset_spec = spec
                break

        if asset_spec is None:
            continue  # Skip if no spec found

        # Create checks using existing utility
        model_checks = create_asset_checks_from_model(model, asset_key)
        assets.append(create_model_asset(model.name, asset_spec, model_checks))

    return assets


def sqlmesh_adaptive_schedule_factory(
    *,
    sqlmesh_resource: SQLMeshResource,
    name: str = "sqlmesh_adaptive_schedule",
):
    """
    Factory to create an adaptive Dagster schedule based on SQLMesh crons.

    Args:
        sqlmesh_resource: Configured SQLMesh resource
        name: Schedule name
    """

    # Get recommended schedule based on SQLMesh crons
    recommended_schedule = sqlmesh_resource.get_recommended_schedule()

    # Create SQLMesh assets (list of individual assets)
    sqlmesh_assets = sqlmesh_assets_factory(sqlmesh_resource=sqlmesh_resource)

    # Check if we have assets
    if not sqlmesh_assets:
        raise ValueError("No SQLMesh assets created - check if models exist")

    # Create job with all assets (no selection needed since we have individual assets)
    # Force run_retries=false to prevent infinite loops with SQLMesh audit failures
    sqlmesh_job = build_sqlmesh_job(sqlmesh_assets, name="sqlmesh_job")

    @schedule(
        job=sqlmesh_job,
        cron_schedule=recommended_schedule,
        name=name,
        description=f"Adaptive schedule based on SQLMesh crons (granularity: {recommended_schedule})",
    )
    def _sqlmesh_adaptive_schedule(context):
        # Enforce instance-level configuration: QueuedRunCoordinator required
        _assert_instance_has_queued_run_coordinator(context.instance)

        # Prevent concurrent scheduler-triggered runs for this job
        active = context.instance.get_runs(
            filters=RunsFilter(
                job_name=sqlmesh_job.name,
                statuses=[
                    DagsterRunStatus.QUEUED,
                    DagsterRunStatus.SUBMITTED,
                    DagsterRunStatus.NOT_STARTED,
                    DagsterRunStatus.STARTING,
                    DagsterRunStatus.STARTED,
                    DagsterRunStatus.CANCELING,
                ],
            )
        )
        if active:
            return SkipReason(
                "sqlmesh job already active; skipping new run to enforce singleton execution"
            )

        scheduled_ts = context.scheduled_execution_time or datetime.datetime.now()
        return RunRequest(
            run_key=f"sqlmesh_adaptive_{scheduled_ts.isoformat()}",
            tags={
                "schedule": "sqlmesh_adaptive",
                "granularity": recommended_schedule,
                "dagster/max_retries": "0",
                "dagster/retry_on_asset_or_op_failure": "false",
                "dagster/concurrency_key": "sqlmesh_jobs_exclusive",
            },
        )

    return _sqlmesh_adaptive_schedule, sqlmesh_job, sqlmesh_assets


def sqlmesh_definitions_factory(
    *,
    project_dir: str = "sqlmesh_project",
    gateway: str = "postgres",
    environment: str = "prod",
    concurrency_limit: int = 1,
    translator: Optional[SQLMeshTranslator] = None,
    external_asset_mapping: Optional[str] = None,
    group_name: str = "sqlmesh",
    op_tags: Optional[Dict[str, Any]] = None,
    owners: Optional[List[str]] = None,
    schedule_name: str = "sqlmesh_adaptive_schedule",
    enable_schedule: bool = False,  # Disable schedule by default
):
    """
    All-in-one factory to create a complete SQLMesh integration with Dagster.

    Args:
        project_dir: SQLMesh project directory
        gateway: SQLMesh gateway (postgres, duckdb, etc.)
        concurrency_limit: Concurrency limit
        translator: Custom translator for asset keys (takes priority over external_asset_mapping)
        external_asset_mapping: Jinja2 template for mapping external assets to Dagster asset keys
            Example: "target/main/{node.name}" or "sling/{node.database}/{node.schema}/{node.name}"
            Variables available: {node.database}, {node.schema}, {node.name}, {node.fqn}
        group_name: Default group for assets
        op_tags: Operation tags
        owners: Asset owners
        schedule_name: Adaptive schedule name
        enable_schedule: Whether to enable the adaptive schedule (default: False)

    Note:
        If both 'translator' and 'external_asset_mapping' are provided, the custom translator
        will be used and a warning will be issued.
    """

    # Parameter validation
    if concurrency_limit < 1:
        raise ValueError("concurrency_limit must be >= 1")

    # Note: Instance-level coordinator enforcement happens reliably at schedule tick time.
    # Attempting to validate here at Definitions load is unreliable across environments,
    # so we intentionally do not perform the check at this stage.

    # Handle translator and external_asset_mapping conflicts
    if translator is not None and external_asset_mapping is not None:
        warnings.warn(
            "⚠️  CONFLICT DETECTED: Both 'translator' and 'external_asset_mapping' are provided.\n"
            "   → Using the custom translator (translator parameter)\n"
            "   → Ignoring external_asset_mapping parameter\n"
            "   → To use external_asset_mapping, remove the translator parameter\n"
            "   → To use custom translator, remove the external_asset_mapping parameter\n"
            "   → Example: sqlmesh_definitions_factory(external_asset_mapping='target/main/{node.name}')",
            UserWarning,
            stacklevel=2,
        )
    elif external_asset_mapping is not None:
        # Create JinjaSQLMeshTranslator from the template
        from .components.sqlmesh_project.component import JinjaSQLMeshTranslator

        translator = JinjaSQLMeshTranslator(external_asset_mapping)
    elif translator is None:
        # Use default translator
        translator = SQLMeshTranslator()

    # Robust default values
    op_tags = op_tags or {"sqlmesh": "true"}
    owners = owners or []

    # Create SQLMesh resource
    sqlmesh_resource = SQLMeshResource(
        project_dir=project_dir,
        gateway=gateway,
        environment=environment,
        translator=translator,
        concurrency_limit=concurrency_limit,
    )

    # Create SQLMesh results resource for sharing between assets
    sqlmesh_results_resource = SQLMeshResultsResource()

    # Validate external dependencies
    try:
        models = sqlmesh_resource.get_models()
        validation_errors = validate_external_dependencies(sqlmesh_resource, models)
        if validation_errors:
            raise ValueError(
                "External dependencies validation failed:\n"
                + "\n".join(validation_errors)
            )
    except Exception as e:
        raise ValueError(f"Failed to validate external dependencies: {e}") from e

    # Create SQLMesh assets
    sqlmesh_assets = sqlmesh_assets_factory(
        sqlmesh_resource=sqlmesh_resource,
        group_name=group_name,
        op_tags=op_tags,
        owners=owners,
    )

    # Create adaptive schedule and job (only if enabled)
    schedules = []
    jobs = []

    if enable_schedule:
        sqlmesh_adaptive_schedule, sqlmesh_job, _ = sqlmesh_adaptive_schedule_factory(
            sqlmesh_resource=sqlmesh_resource, name=schedule_name
        )
        schedules.append(sqlmesh_adaptive_schedule)
        jobs.append(sqlmesh_job)
    else:
        jobs.append(build_sqlmesh_job(sqlmesh_assets, name="sqlmesh_job"))

    # Return complete Definitions
    return Definitions(
        assets=sqlmesh_assets,
        jobs=jobs,
        schedules=schedules,
        resources={
            "sqlmesh": sqlmesh_resource,
            "sqlmesh_results": sqlmesh_results_resource,
        },
    )
