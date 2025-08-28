"""
Execution utilities for SQLMesh assets.

Functions extracted from the `model_asset` flow to improve readability and
testability. All docstrings and logs are standardized in English.
"""

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
)
from typing import Dict, List, Any, Tuple
from .resource import SQLMeshResource
from .sqlmesh_asset_utils import get_models_to_materialize
from .resource import UpstreamAuditFailureError
from .execution_notifier import (
    _get_notifier_failures as _get_notifier_failures_noarg,
)
from .notifier_service import clear_notifier_state, get_audit_failures
from .execution_selection import (
    _log_run_selection as _log_run_selection_ext,
    _materialize_and_get_plan as _materialize_and_get_plan_ext,
)
from .execution_check_results import (
    _build_failed_check_results_for_all_checks,
    _get_blocking_and_non_blocking_names_for_model,
    _build_check_result_failed_from_notifier,
    _build_check_results_for_create_result,
)
from .execution_results_payload import (
    _init_execution_event_buffers as _init_execution_event_buffers_ext,
    _build_shared_results as _build_shared_results_ext,
)
from .simple_run_tracker import sqlmesh_run_tracker

# ----------------------------- Internal helpers (Phase 1) -----------------------------


def _log_run_selection(
    context: AssetExecutionContext, run_id: str, selected_asset_keys: List[AssetKey]
) -> None:
    _log_run_selection_ext(context, run_id, selected_asset_keys)


def _select_models_to_materialize(
    selected_asset_keys: List[AssetKey], sqlmesh: SQLMeshResource
) -> List[Any]:
    """Resolve SQLMesh models from selection.

    Kept here to preserve test monkeypatching of `get_models_to_materialize` symbol
    in this module.
    """
    models_to_materialize = get_models_to_materialize(
        selected_asset_keys,
        sqlmesh.get_models,
        sqlmesh.translator,
    )
    if not models_to_materialize:
        raise Exception(f"No models found for selected assets: {selected_asset_keys}")
    return models_to_materialize


def _materialize_and_get_plan(
    sqlmesh: SQLMeshResource,
    models_to_materialize: List[Any],
    context: AssetExecutionContext,
) -> Any:
    return _materialize_and_get_plan_ext(sqlmesh, models_to_materialize, context)


def _init_execution_event_buffers(
    context: AssetExecutionContext,
) -> tuple[List[AssetCheckResult], List[Dict], List[Dict], List[Dict]]:
    context.log.debug("Failed check results count: 0")
    context.log.debug("Processing skipped models events... (skipped, console disabled)")
    context.log.debug("Evaluation events count: 0")
    return _init_execution_event_buffers_ext()


def _get_notifier_failures(
    _: SQLMeshResource | None = None,
) -> List[Dict]:
    """Compatibility shim delegating to the no-arg notifier helper.

    Tests may call this with an argument; ignore it.
    """
    try:
        return _get_notifier_failures_noarg()
    except Exception:
        return []


def _build_shared_results(
    plan: Any,
    failed_check_results: List[AssetCheckResult],
    skipped_models_events: List[Dict],
    evaluation_events: List[Dict],
    non_blocking_audit_warnings: List[Dict],
    notifier_audit_failures: List[Dict],
    affected_downstream_asset_keys: set[AssetKey],
) -> Dict[str, Any]:
    return _build_shared_results_ext(
        plan,
        failed_check_results,
        skipped_models_events,
        evaluation_events,
        non_blocking_audit_warnings,
        notifier_audit_failures,
        affected_downstream_asset_keys,
    )


def _parse_snapshot_to_model_name(snapshot_name: str) -> str | None:
    """Convert a snapshot name '"db"."schema"."model"' to 'schema.model'."""
    try:
        parts = snapshot_name.split('"."')
        if len(parts) >= 3:
            return parts[1] + "." + parts[2].replace('"', "")
    except Exception:
        return None
    return None


def _model_was_skipped_from_events(
    skipped_models_events: List[Dict],
    current_model_name: str,
    logger: Any | None = None,
) -> bool:
    """Check if the current model appears in the skipped events."""
    for event in skipped_models_events:
        skipped_snapshots = event.get("snapshot_names", set())
        for snapshot_name in skipped_snapshots:
            if not snapshot_name:
                continue
            skipped_model_name = _parse_snapshot_to_model_name(snapshot_name)

            if skipped_model_name == current_model_name:
                return True
    return False


def _model_has_failed_audits_for_asset(
    failed_check_results: List[AssetCheckResult],
    current_asset_spec_key: Any,
    current_model_name: str,
    logger: Any | None = None,
) -> bool:
    """Check if any failed check result targets the current asset key."""
    for check_result in failed_check_results:
        if check_result.asset_key == current_asset_spec_key:
            if logger:
                logger.error(
                    f"Model {current_model_name} has audit failures: {check_result.metadata.get('audit_message', 'Unknown error')}"
                )
            return True
    return False


# All check result functions are imported from execution_check_results module


def execute_sqlmesh_materialization(
    context: AssetExecutionContext,
    sqlmesh: SQLMeshResource,
    sqlmesh_results: Any,
    run_id: str,
    selected_asset_keys: List[AssetKey],
) -> Dict[str, Any]:
    """
    Execute a single SQLMesh materialization for all selected assets (shared execution).

    Args:
        context: Dagster execution context
        sqlmesh: SQLMesh resource
        sqlmesh_results: Shared results resource
        run_id: Dagster run identifier
        selected_asset_keys: Selected assets in this run

    Returns:
        Dict with captured execution results for later reuse in the same run
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
    # Console removed → no legacy failed models events
    # Console disabled path
    # Initialize result buffers (console disabled)
    failed_check_results: List[AssetCheckResult] = []
    skipped_models_events: List[Dict] = []
    evaluation_events: List[Dict] = []
    non_blocking_audit_warnings: List[Dict] = []

    # Store results in the shared resource
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

    # Build shared result payload
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

    sqlmesh_results.store_results(run_id, results)
    # Note: Do NOT clear notifier state here as it contains audit failures
    # that need to be retrieved by the caller for check result creation

    return results


def process_sqlmesh_results(
    context: AssetExecutionContext, sqlmesh_results: Any, run_id: str
) -> (
    Tuple[List[AssetCheckResult], List[Dict], List[Dict]]
    | Tuple[List[AssetCheckResult], List[Dict], List[Dict], List[Dict], List[AssetKey]]
    | Tuple[
        List[AssetCheckResult],
        List[Dict],
        List[Dict],
        List[Dict],
        List[AssetKey],
        List[str],
    ]
):
    """
    Retrieve and process shared SQLMesh results for this run.

    Returns a tuple:
      - failed_check_results
      - skipped_models_events
      - non_blocking_audit_warnings
      - notifier_audit_failures
      - affected_downstream_asset_keys
      - sqlmesh_skipped_models
    """
    # Retrieve results for this run
    results = sqlmesh_results.get_results(run_id)
    if results is None:
        context.log.error("No results found in sqlmesh_results for run %s", run_id)
        return [], [], [], [], []
    failed_check_results = results.get("failed_check_results", [])
    skipped_models_events = results.get("skipped_models_events", [])
    # Backward-compat: if legacy shape is present, return the 3-tuple expected by older tests
    if "evaluation_events" in results and "non_blocking_audit_warnings" not in results:
        evaluation_events = results.get("evaluation_events", [])
        return failed_check_results, skipped_models_events, evaluation_events
    non_blocking_audit_warnings = results.get("non_blocking_audit_warnings", [])
    notifier_audit_failures = results.get("notifier_audit_failures", [])
    affected_downstream_asset_keys = results.get("affected_downstream_asset_keys", [])
    sqlmesh_executed_models = results.get("sqlmesh_executed_models", [])
    sqlmesh_skipped_models = results.get("sqlmesh_skipped_models", [])

    context.log.info(
        f"Retrieved results: failed={len(failed_check_results)}, skipped={len(skipped_models_events)}, nb_warn={len(non_blocking_audit_warnings)}, notifier_failures={len(notifier_audit_failures)}"
    )

    return (
        failed_check_results,
        skipped_models_events,
        non_blocking_audit_warnings,
        notifier_audit_failures,
        affected_downstream_asset_keys,
        sqlmesh_executed_models,  # Add SQLMesh executed models
        sqlmesh_skipped_models,  # Add SQLMesh skipped models
    )


def check_model_status(
    context: AssetExecutionContext,
    current_model_name: str,
    current_asset_spec: Any,
    failed_check_results: List[AssetCheckResult],
    skipped_models_events: List[Dict],
) -> Tuple[bool, bool]:
    """
    Check the status of a specific model.

    Returns a tuple: (model_was_skipped, model_has_audit_failures)
    """
    model_was_skipped = False
    model_has_audit_failures = False

    # Check if skipped due to upstream failures
    if _model_was_skipped_from_events(
        skipped_models_events, current_model_name, logger=context.log
    ):
        model_was_skipped = True
        context.log.error(
            f"Model {current_model_name} was skipped due to upstream failures"
        )

    # Check audit failures (model executed but audit failed)
    if _model_has_failed_audits_for_asset(
        failed_check_results,
        current_asset_spec.key,
        current_model_name,
        logger=context.log,
    ):
        model_has_audit_failures = True

    context.log.debug(
        f"Model {current_model_name} - was_skipped: {model_was_skipped}, has_audit_failures: {model_has_audit_failures}"
    )

    return model_was_skipped, model_has_audit_failures


def handle_audit_failures(
    context: AssetExecutionContext,
    current_model_name: str,
    current_asset_spec: Any,
    current_model_checks: List[Any],
    failed_check_results: List[AssetCheckResult],
) -> MaterializeResult:
    """
    Handle the case where the model executed but audits failed.

    Returns a MaterializeResult with failed checks populated.
    """
    context.log.info(
        f"Model {current_model_name}: materialization succeeded but at least one audit failed"
    )
    context.log.debug("Returning MaterializeResult with failed checks")

    # If checks exist, return their results
    if current_model_checks:
        check_results = _build_failed_check_results_for_all_checks(
            current_model_checks=current_model_checks,
            current_asset_spec_key=current_asset_spec.key,
            failed_check_results=failed_check_results,
            current_model_name=current_model_name,
            logger=context.log,
        )

        return MaterializeResult(
            asset_key=current_asset_spec.key,
            metadata={"status": "materialization_success_audit_failed"},
            check_results=check_results,
        )
    else:
        context.log.warning(
            f"No checks defined for model {current_model_name}; returning only MaterializeResult"
        )
        return MaterializeResult(
            asset_key=current_asset_spec.key,
            metadata={"status": "materialization_success_audit_failed"},
        )


def handle_successful_execution(
    context: AssetExecutionContext,
    current_model_name: str,
    current_asset_spec: Any,
    current_model_checks: List[Any],
    non_blocking_audit_warnings: List[Dict] | None = None,
    notifier_audit_failures: List[Dict] | None = None,
    sqlmesh_executed_models: List[str] | None = None,
    sqlmesh_skipped_models: List[str] | None = None,
) -> MaterializeResult:
    """
    Handle the case where the model executed successfully.

    Returns a MaterializeResult with passed checks (and WARN for non-blocking failures).
    """
    context.log.info(f"Model {current_model_name}: success")

    # Normalize optional inputs
    non_blocking_audit_warnings = non_blocking_audit_warnings or []
    notifier_audit_failures = notifier_audit_failures or []
    sqlmesh_executed_models = sqlmesh_executed_models or []
    sqlmesh_skipped_models = sqlmesh_skipped_models or []

    # If checks exist, return their results
    if current_model_checks:
        check_results: List[AssetCheckResult] = []

        # Build failing set from notifier and warnings for current model
        blocking_names, non_blocking_names, _failed_for_model = (
            _get_blocking_and_non_blocking_names_for_model(
                notifier_audit_failures, non_blocking_audit_warnings, current_model_name
            )
        )

        # Emit WARN failed for non-blocking failures, PASS for others
        for check in current_model_checks:
            # Skip sqlmesh_execution_status - we handle it separately below
            if check.name == "sqlmesh_execution_status":
                continue

            if check.name in non_blocking_names:
                fail = next(
                    (
                        f
                        for f in notifier_audit_failures
                        if f.get("model") == current_model_name
                        and f.get("audit") == check.name
                    ),
                    {},
                )
                check_results.append(
                    _build_check_result_failed_from_notifier(
                        check_name=check.name,
                        current_model_name=current_model_name,
                        notifier_record=fail,
                        blocking=False,
                        context=context,
                    )
                )
            else:
                # Use the existing check's metadata instead of rebuilding it
                # This preserves the correct blocking status determined during check creation
                from .execution_check_results import get_check_severity_for_blocking

                check_results.append(
                    AssetCheckResult(
                        passed=True,
                        severity=get_check_severity_for_blocking(
                            check.metadata.get("audit_blocking", True)
                        ),
                        check_name=check.name,
                        metadata=check.metadata,
                    )
                )

        # Handle execution status check for ALL models
        # Check if this model was executed by SQLMesh (using tracker results)
        model_was_executed_by_sqlmesh = current_model_name in sqlmesh_executed_models

        # Find the execution status check spec
        execution_status_check = next(
            (
                check
                for check in current_model_checks
                if check.name == "sqlmesh_execution_status"
            ),
            None,
        )

        if execution_status_check:
            if model_was_executed_by_sqlmesh:
                # Model was executed → SUCCESS
                check_results.append(
                    AssetCheckResult(
                        passed=True,
                        severity=AssetCheckSeverity.WARN,  # Use WARN since INFO doesn't exist
                        check_name="sqlmesh_execution_status",
                        metadata={
                            "sqlmesh_model": current_model_name,
                            "check_type": "execution_status",
                            "status": "executed",
                            "message": f"Model {current_model_name} was executed by SQLMesh",
                        },
                    )
                )
            else:
                # Model was skipped → WARNING (user should know)
                check_results.append(
                    AssetCheckResult(
                        passed=False,  # Warning = failed check
                        severity=AssetCheckSeverity.WARN,
                        check_name="sqlmesh_execution_status",
                        metadata={
                            "sqlmesh_model": current_model_name,
                            "check_type": "execution_status",
                            "status": "skipped",
                            "message": f"Model {current_model_name} was skipped by SQLMesh (no new data to compute)",
                        },
                    )
                )

        return MaterializeResult(
            asset_key=current_asset_spec.key,
            metadata={"status": "success"},
            check_results=check_results,
        )
    else:
        return MaterializeResult(
            asset_key=current_asset_spec.key, metadata={"status": "success"}
        )


def create_materialize_result(
    context: AssetExecutionContext,
    current_model_name: str,
    current_asset_spec: Any,
    current_model_checks: List[Any],
    model_was_skipped: bool,
    model_has_audit_failures: bool,
    non_blocking_audit_warnings: List[Dict] | None = None,
    notifier_audit_failures: List[Dict] | None = None,
    affected_downstream_asset_keys: List[AssetKey] | None = None,
    sqlmesh_executed_models: List[str] | None = None,
    sqlmesh_skipped_models: List[str] | None = None,
    *,
    # Legacy keyword-only params for backward compatibility with older tests
    failed_check_results: List[AssetCheckResult] | None = None,
    evaluation_events: List[Dict] | None = None,
) -> MaterializeResult:
    """
    Create the appropriate MaterializeResult based on the model status.

    Returns the correct result or raises UpstreamAuditFailureError for skipped/blocked cases.
    """

    # Normalize optional inputs
    non_blocking_audit_warnings = non_blocking_audit_warnings or []
    notifier_audit_failures = notifier_audit_failures or []
    affected_downstream_asset_keys = affected_downstream_asset_keys or []
    # Legacy params intentionally ignored in new flow; kept for API compatibility
    _ = failed_check_results, evaluation_events

    if model_was_skipped:
        # Skipped model → raise an exception (no materialization)
        error_msg = f"Model {current_model_name} was skipped due to upstream failures"
        context.log.error(error_msg)
        context.log.debug("Raising UpstreamAuditFailureError for skipped model")
        raise UpstreamAuditFailureError(description=error_msg)
    elif model_has_audit_failures or any(
        f.get("blocking") and f.get("model") == current_model_name
        for f in notifier_audit_failures
    ):
        context.log.info(
            f"Creating failed MaterializeResult for {current_model_name} due to blocking audit failure"
        )

        # Build precise check results via centralized helper
        check_results = _build_check_results_for_create_result(
            current_model_checks=current_model_checks,
            current_model_name=current_model_name,
            notifier_audit_failures=notifier_audit_failures,
            non_blocking_audit_warnings=non_blocking_audit_warnings,
            context=context,
        )

        result = MaterializeResult(
            asset_key=current_asset_spec.key,
            metadata={"status": "materialization_success_audit_failed"},
            check_results=check_results,
        )
        return result
    else:
        # If current asset is unaffected but is in affected downstream set, raise to block
        if current_asset_spec.key in set(affected_downstream_asset_keys):
            # Block following the upstream failure pattern
            context.log.info(
                f"Blocking downstream materialization for {current_model_name} due to upstream failures"
            )
            raise UpstreamAuditFailureError(
                description=f"Asset {current_asset_spec.key} skipped due to upstream audit failures"
            )

        return handle_successful_execution(
            context,
            current_model_name,
            current_asset_spec,
            current_model_checks,
            non_blocking_audit_warnings,
            notifier_audit_failures,
            sqlmesh_executed_models,
            sqlmesh_skipped_models,
        )
