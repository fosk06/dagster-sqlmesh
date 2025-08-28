from __future__ import annotations

from typing import Dict, List
from .notifier_service import get_audit_failures


def _get_notifier_failures() -> List[
    Dict
]:  # TODO check if still in use and find if another method replace it somewhere
    """Safely retrieve notifier audit failures via notifier service; return empty list on error."""
    try:
        return get_audit_failures()
    except Exception:
        return []
