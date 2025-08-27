from __future__ import annotations

from typing import Dict, List, Set, Tuple
from dagster import AssetKey

from .resource import SQLMeshResource


def _compute_blocking_and_downstream(  # TODO check if still in use and find if another method replace it somewhere
    sqlmesh: SQLMeshResource, notifier_audit_failures: List[Dict]
) -> Tuple[List[AssetKey], Set[AssetKey]]:
    """Compute failing blocking asset keys and affected downstream asset keys."""
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

    # Ensure we don't include the failing assets themselves in the downstream set
    try:
        affected_downstream_asset_keys = set(affected_downstream_asset_keys) - set(
            blocking_failed_asset_keys
        )
    except Exception:
        affected_downstream_asset_keys = set()

    return blocking_failed_asset_keys, affected_downstream_asset_keys
