from __future__ import annotations

from typing import Any, Dict, List, Tuple
from dagster import AssetCheckResult, AssetCheckSeverity, AssetExecutionContext

from .sqlmesh_asset_check_utils import build_audit_check_metadata


def get_check_severity_for_blocking(is_blocking: bool) -> AssetCheckSeverity:
    return AssetCheckSeverity.ERROR if is_blocking else AssetCheckSeverity.WARN


def _build_failed_check_results_for_all_checks(
    current_model_checks: List[Any],
    current_asset_spec_key: Any,
    failed_check_results: List[AssetCheckResult],
    current_model_name: str,
    logger: Any | None = None,
) -> List[AssetCheckResult]:
    results: List[AssetCheckResult] = []
    for check in current_model_checks:
        audit_message = "Model materialization succeeded but audits failed"
        for check_result in failed_check_results:
            if check_result.asset_key == current_asset_spec_key:
                audit_message = check_result.metadata.get(
                    "audit_message", audit_message
                )
                break
        result = AssetCheckResult(
            check_name=check.name,
            passed=False,
            metadata={
                "audit_message": audit_message,
                "sqlmesh_audit_name": check.name,
                "sqlmesh_model": current_model_name,
                "error_details": f"SQLMesh audit '{check.name}' failed: {audit_message}",
            },
        )
        results.append(result)
        if logger:
            logger.debug(
                f"Created failed check result for: {check.name} with message: {audit_message}"
            )
    return results


def _get_blocking_and_non_blocking_names_for_model(
    notifier_audit_failures: List[Dict],
    non_blocking_audit_warnings: List[Dict],
    current_model_name: str,
) -> Tuple[set[str], set[str], List[Dict]]:
    failed_for_model = [
        f for f in notifier_audit_failures if f.get("model") == current_model_name
    ]
    blocking_names = {f.get("audit") for f in failed_for_model if f.get("blocking")}
    non_blocking_names = {
        f.get("audit") for f in failed_for_model if not f.get("blocking")
    }
    for w in non_blocking_audit_warnings:
        if w.get("model_name") == current_model_name:
            non_blocking_names.add(w.get("audit_name"))
    return blocking_names, non_blocking_names, failed_for_model


def _build_check_result_failed_from_notifier(
    *,
    check_name: str,
    current_model_name: str,
    notifier_record: Dict[str, Any] | None,
    blocking: bool,
    context: AssetExecutionContext,
) -> AssetCheckResult:
    safe_record = {
        **(notifier_record or {}),
        "model": current_model_name,
        "audit": check_name,
        "blocking": blocking,
    }
    metadata = build_audit_check_metadata(
        context=context.resources.sqlmesh.context
        if hasattr(context.resources, "sqlmesh")
        else None,  # type: ignore[attr-defined]
        model_or_name=current_model_name,
        audit_name=check_name,
        notifier_record=safe_record,
        logger=getattr(context, "log", None),
    )
    return AssetCheckResult(
        check_name=check_name,
        passed=False,
        severity=get_check_severity_for_blocking(blocking),
        metadata=metadata,
    )


def _build_check_results_for_create_result(
    *,
    current_model_checks: List[Any],
    current_model_name: str,
    notifier_audit_failures: List[Dict],
    non_blocking_audit_warnings: List[Dict],
    context: AssetExecutionContext,
) -> List[AssetCheckResult]:
    check_results: List[AssetCheckResult] = []
    blocking_names, non_blocking_names, failed_for_model = (
        _get_blocking_and_non_blocking_names_for_model(
            notifier_audit_failures, non_blocking_audit_warnings, current_model_name
        )
    )

    for check in current_model_checks:
        if check.name in blocking_names:
            fail = next(
                (f for f in failed_for_model if f.get("audit") == check.name), {}
            )
            check_results.append(
                _build_check_result_failed_from_notifier(
                    check_name=check.name,
                    current_model_name=current_model_name,
                    notifier_record=fail,
                    blocking=True,
                    context=context,
                )
            )
        elif check.name in non_blocking_names:
            fail_nb = next(
                (
                    f
                    for f in failed_for_model
                    if not f.get("blocking") and f.get("audit") == check.name
                ),
                {},
            )
            check_results.append(
                _build_check_result_failed_from_notifier(
                    check_name=check.name,
                    current_model_name=current_model_name,
                    notifier_record=fail_nb,
                    blocking=False,
                    context=context,
                )
            )
        else:
            # Use the existing check's metadata instead of rebuilding it
            # This preserves the correct blocking status determined during check creation
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

    return check_results
