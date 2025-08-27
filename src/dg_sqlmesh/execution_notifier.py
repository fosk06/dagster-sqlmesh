from __future__ import annotations

from typing import Dict, List
from dagster import AssetExecutionContext
from .notifier_service import get_audit_failures


def _get_notifier_failures() -> List[
    Dict
]:  # TODO check if still in use and find if another method replace it somewhere
    """Safely retrieve notifier audit failures via notifier service; return empty list on error."""
    try:
        return get_audit_failures()
    except Exception:
        return []


def _summarize_notifier_failures(  # TODO check if still in use and find if another method replace it somewhere
    context: AssetExecutionContext, notifier_audit_failures: List[Dict]
) -> None:
    """Log a compact summary of notifier failures if present."""
    if not notifier_audit_failures:
        return
    try:
        summary = [
            {
                "model": f.get("model"),
                "audit": f.get("audit"),
                "blocking": f.get("blocking"),
                "count": f.get("count"),
            }
            for f in notifier_audit_failures
        ]
        context.log.info(f"Notifier audit failures summary: {summary}")
    except Exception:
        # ignore logging issues to avoid breaking execution
        pass
