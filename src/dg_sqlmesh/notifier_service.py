from __future__ import annotations

from typing import Any

from sqlmesh import Context

from .notifier import CapturingNotifier


_NOTIFIER_SINGLETON: CapturingNotifier | None = None


def get_or_create_notifier(logger=None) -> CapturingNotifier:
    """Return a process-wide singleton instance of CapturingNotifier."""
    global _NOTIFIER_SINGLETON
    if _NOTIFIER_SINGLETON is None:
        _NOTIFIER_SINGLETON = CapturingNotifier()
    if logger is not None:
        _NOTIFIER_SINGLETON.set_logger(logger)
    return _NOTIFIER_SINGLETON


def register_notifier_in_context(context: Context, logger=None) -> None:
    """Ensure the notifier is registered within the provided SQLMesh Context.

    This function is idempotent and safe against changes in SQLMesh internal APIs.
    """
    try:
        notifier = get_or_create_notifier(logger=logger)
        # Lazily create list if missing and avoid duplicate registration
        targets = getattr(context, "notification_targets", None)
        if targets is None:
            try:
                context.notification_targets = []  # type: ignore[attr-defined]
            except Exception:
                # If the attribute is not assignable, just continue and attempt append
                pass

        targets = getattr(context, "notification_targets", [])
        if notifier not in targets:
            try:
                targets.append(notifier)
                context.notification_targets = targets  # type: ignore[attr-defined]
            except Exception:
                # Best-effort: try to append even if assignment blocked
                try:
                    context.notification_targets.append(notifier)  # type: ignore[attr-defined]
                except Exception:
                    pass

        # Re-register targets if SQLMesh provides an internal helper
        if hasattr(context, "_register_notification_targets"):
            try:
                context._register_notification_targets()  # type: ignore[attr-defined]
            except Exception:
                # Avoid breaking execution if SQLMesh internals change
                pass
    except Exception:
        # Never fail context bootstrap due to notifier registration
        return None


def get_audit_failures() -> list[dict[str, Any]]:
    """Return captured audit failures from the singleton notifier."""
    try:
        return get_or_create_notifier().get_audit_failures()
    except Exception:
        return []


def get_run_events() -> list[dict[str, Any]]:
    try:
        return get_or_create_notifier().get_run_events()
    except Exception:
        return []


def get_apply_events() -> list[dict[str, Any]]:
    try:
        return get_or_create_notifier().get_apply_events()
    except Exception:
        return []


def clear_notifier_state() -> None:
    """Clear the singleton notifier captured state (tests isolation)."""
    try:
        notifier = get_or_create_notifier()
        notifier.clear()
    except Exception:
        return None
