from dagster import (
    asset,
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    AssetCheckResult,
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
    handle_successful_execution,
)
from .sqlmesh_asset_utils import get_models_to_materialize
from .sqlmesh_schedule_utils import should_skip_sqlmesh_run
from .notifier_service import clear_notifier_state, get_audit_failures
from .simple_run_tracker import sqlmesh_run_tracker


# -------------------- Precompute Asset for Single SQLMesh Execution --------------------


def _execute_sqlmesh_materialization_for_precompute(
    context: AssetExecutionContext,
    sqlmesh: SQLMeshResource,
    selected_asset_keys: List[AssetKey],
) -> Dict[str, Any]:
    """
    Execute SQLMesh materialization for precompute asset.
    Reuses existing logic from execute_sqlmesh_materialization but without SQLMeshResultsResource.
    """
    # Resolve models to materialize
    models_to_materialize = get_models_to_materialize(
        selected_asset_keys,
        sqlmesh.get_models,
        sqlmesh.translator,
    )
    if not models_to_materialize:
        raise Exception(f"No models found for selected assets: {selected_asset_keys}")

    # Single SQLMesh execution
    context.log.info(
        f"Materializing {len(models_to_materialize)} models: {[m.name for m in models_to_materialize]}"
    )
    context.log.debug(
        "Starting SQLMesh materialization (count=%d)", len(models_to_materialize)
    )

    # Clear notifier state at run start to avoid accumulating audit failures from previous runs
    try:
        clear_notifier_state()
        context.log.debug("Notifier state cleared at run start")
    except Exception:
        pass

    # Use context manager for clean console tracking
    with sqlmesh_run_tracker(sqlmesh.context) as tracker:
        plan = sqlmesh.materialize_assets_threaded(
            models_to_materialize, context=context
        )
        context.log.debug("SQLMesh materialization completed")

        # Get executed models from tracker
        sqlmesh_executed_models = tracker.get_executed_models()

        # DEDUCTION LOGIC: Models skipped = Models requested - Models executed
        # This handles cron-based skips that our tracker doesn't capture directly
        requested_models = [model.name for model in models_to_materialize]

        # Normalize executed model names to match requested format
        # Remove quotes and database prefix to match model.name format
        normalized_executed_models = []
        for executed_model in sqlmesh_executed_models:
            # Remove quotes and split by dots
            clean_name = executed_model.replace('"', "")
            parts = clean_name.split(".")
            if len(parts) >= 3:
                # Keep only schema.model format (skip database)
                normalized_name = f"{parts[1]}.{parts[2]}"
                normalized_executed_models.append(normalized_name)
            else:
                normalized_executed_models.append(clean_name)

        sqlmesh_skipped_models = list(
            set(requested_models) - set(normalized_executed_models)
        )

        context.log.info(
            f"Execution deduction: {len(requested_models)} requested, {len(sqlmesh_executed_models)} executed, {len(sqlmesh_skipped_models)} deduced as skipped"
        )

    # Capture all results
    # Initialize result buffers (console disabled)
    failed_check_results: List[AssetCheckResult] = []
    skipped_models_events: List[Dict] = []
    evaluation_events: List[Dict] = []
    non_blocking_audit_warnings: List[Dict] = []

    # Capture audit failures from the notifier (robust)
    # Get notifier failures via service and log summary
    notifier_audit_failures = get_audit_failures()

    # Build blocking AssetKeys and affected downstream assets
    # Compute blocking and downstream
    blocking_failed_asset_keys: List[AssetKey] = []
    try:
        for fail in notifier_audit_failures:
            if fail.get("blocking") and fail.get("model"):
                model = sqlmesh.context.get_model(fail.get("model"))
                if model:
                    blocking_failed_asset_keys.append(
                        sqlmesh.translator.get_asset_key(model)
                    )
    except Exception:
        pass

    try:
        affected_downstream_asset_keys = sqlmesh._get_affected_downstream_assets(
            blocking_failed_asset_keys
        )
    except Exception:
        affected_downstream_asset_keys = set()

    try:
        affected_downstream_asset_keys = set(affected_downstream_asset_keys) - set(
            blocking_failed_asset_keys
        )
    except Exception:
        affected_downstream_asset_keys = set()

    context.log.info(
        f"Blocking failed assets: {blocking_failed_asset_keys} | Downstream affected: {list(affected_downstream_asset_keys)}"
    )

    # Build result payload
    results: Dict[str, Any] = {
        "failed_check_results": failed_check_results,
        "skipped_models_events": skipped_models_events,
        "evaluation_events": evaluation_events,
        "non_blocking_audit_warnings": non_blocking_audit_warnings,
        "notifier_audit_failures": notifier_audit_failures,
        "affected_downstream_asset_keys": list(affected_downstream_asset_keys),
        "sqlmesh_executed_models": normalized_executed_models,  # Store NORMALIZED executed models
        "sqlmesh_skipped_models": sqlmesh_skipped_models,  # Models actually skipped by SQLMesh
        "plan": plan,
    }

    return results


def _create_success_result(
    context: AssetExecutionContext,
    model_name: str,
    asset_spec: Any,
    model_checks: List[Any],
    sqlmesh_execution_results: Dict[str, Any],
    sqlmesh: SQLMeshResource,
) -> MaterializeResult:
    """Create MaterializeResult for successfully executed model."""
    audit_results = sqlmesh_execution_results["audit_results"]

    return handle_successful_execution(
        context=context,
        current_model_name=model_name,
        current_asset_spec=asset_spec,
        current_model_checks=model_checks,
        non_blocking_audit_warnings=audit_results.get("non_blocking_warnings", []),
        notifier_audit_failures=audit_results.get("notifier_failures", []),
        sqlmesh_executed_models=sqlmesh_execution_results["model_status"]["executed"],
        sqlmesh_skipped_models=sqlmesh_execution_results["model_status"]["skipped"],
    )


def _create_skipped_result(
    context: AssetExecutionContext,
    model_name: str,
    asset_spec: Any,
    model_checks: List[Any],
    sqlmesh_execution_results: Dict[str, Any],
    sqlmesh: SQLMeshResource,
) -> MaterializeResult:
    """Create MaterializeResult for skipped model."""
    from .resource import UpstreamAuditFailureError

    # For skipped models, raise UpstreamAuditFailureError
    # This will be handled gracefully by Dagster
    raise UpstreamAuditFailureError(
        f"Model {model_name} was skipped by SQLMesh due to upstream dependencies or cron scheduling"
    )


def _create_not_found_result(
    context: AssetExecutionContext,
    model_name: str,
    asset_spec: Any,
    model_checks: List[Any],
    sqlmesh_execution_results: Dict[str, Any],
    sqlmesh: SQLMeshResource,
) -> MaterializeResult:
    """Create MaterializeResult for model not found in results."""
    from .resource import UpstreamAuditFailureError

    # For models not found in results, raise UpstreamAuditFailureError
    raise UpstreamAuditFailureError(
        f"Model {model_name} was not found in SQLMesh execution results"
    )


@asset(
    group_name="sqlmesh_internal",  # Separate group to hide in UI
    tags={"internal": "", "precompute": "", "sqlmesh": ""},
    retry_policy=RetryPolicy(max_retries=0),
)
def sqlmesh_execution_results(
    context: AssetExecutionContext, sqlmesh: SQLMeshResource
) -> Dict[str, Any]:
    """
    Precompute asset that executes SQLMesh once and returns results
    for all selected models. This ensures single SQLMesh execution per run.
    """
    context.log.info("🚀 Starting single SQLMesh execution...")

    # Get all selected assets
    selected_asset_keys = context.selected_asset_keys
    context.log.info(f"📋 Selected assets: {[str(key) for key in selected_asset_keys]}")

    # Convert to SQLMesh models
    models_to_materialize = get_models_to_materialize(
        selected_asset_keys,
        sqlmesh.get_models,
        sqlmesh.translator,
    )

    model_names = [model.name for model in models_to_materialize]
    context.log.info(f"🔧 SQLMesh models to execute: {model_names}")

    try:
        # Execute SQLMesh materialization using existing utility
        # This includes all the console/notifier logic we need
        results = _execute_sqlmesh_materialization_for_precompute(
            context=context,
            sqlmesh=sqlmesh,
            selected_asset_keys=selected_asset_keys,
        )

        context.log.info("✅ SQLMesh execution completed successfully")

        # Structure the results for individual assets
        return {
            "execution_timestamp": datetime.datetime.now().isoformat(),
            "plan": results.get("plan"),
            "model_status": {
                "executed": results.get("sqlmesh_executed_models", []),
                "skipped": results.get("sqlmesh_skipped_models", []),
                "failed": [
                    check.asset_key.path[-1]
                    for check in results.get("failed_check_results", [])
                ],
            },
            "audit_results": {
                "failed_checks": results.get("failed_check_results", []),
                "skipped_models": results.get("skipped_models_events", []),
                "non_blocking_warnings": results.get("non_blocking_audit_warnings", []),
                "notifier_failures": results.get("notifier_audit_failures", []),
            },
            "affected_downstream": list(
                results.get("affected_downstream_asset_keys", set())
            ),
            "assetkey_to_snapshot": results.get("assetkey_to_snapshot", {}),
        }

    except Exception as e:
        context.log.error(f"❌ Error during SQLMesh execution: {str(e)}")
        raise


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

    # Start with the precompute asset
    assets = [sqlmesh_execution_results]

    def create_model_asset(
        current_model_name, current_asset_spec, current_model_checks
    ):
        @asset(
            key=current_asset_spec.key,
            description=f"SQLMesh model: {current_model_name}",
            group_name=current_asset_spec.group_name,
            metadata=current_asset_spec.metadata,
            deps=[sqlmesh_execution_results],  # Direct dependency on precompute asset
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
            sqlmesh_execution_results: Dict[
                str, Any
            ],  # Automatically passed by Dagster
            sqlmesh: SQLMeshResource,
        ):
            # Assert instance-level coordinator enforcement
            _assert_instance_for_compute()

            context.log.info(f"🔍 Processing model: {current_model_name}")

            # Get model status from precompute results
            model_status = sqlmesh_execution_results["model_status"]

            # Check if this model was executed, skipped, or failed
            if current_model_name in model_status["executed"]:
                context.log.info(
                    f"✅ Model {current_model_name} was executed successfully"
                )
                return _create_success_result(
                    context,
                    current_model_name,
                    current_asset_spec,
                    current_model_checks,
                    sqlmesh_execution_results,
                    sqlmesh,
                )
            elif current_model_name in model_status["skipped"]:
                context.log.info(f"⏭️ Model {current_model_name} was skipped by SQLMesh")
                return _create_skipped_result(
                    context,
                    current_model_name,
                    current_asset_spec,
                    current_model_checks,
                    sqlmesh_execution_results,
                    sqlmesh,
                )
            else:
                context.log.warning(
                    f"⚠️ Model {current_model_name} not found in results"
                )
                return _create_not_found_result(
                    context,
                    current_model_name,
                    current_asset_spec,
                    current_model_checks,
                    sqlmesh_execution_results,
                    sqlmesh,
                )

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

        # Use dry-run to check if there are models to execute
        skip_reason = should_skip_sqlmesh_run(sqlmesh_resource, context)
        if skip_reason:
            return skip_reason

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

    # No longer need SQLMeshResultsResource - using Dagster dependencies instead

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
        },
    )
