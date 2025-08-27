from __future__ import annotations

import typing as t

from sqlmesh.core.notification_target import (
    BaseNotificationTarget,
    NotificationStatus,
    NotificationEvent,
)
from sqlmesh.utils.errors import AuditError
from pydantic import PrivateAttr
from pydantic import Field
from .sqlmesh_asset_check_utils import (
    extract_failed_audit_details,
)


class CapturingNotifier(BaseNotificationTarget):
    """
    Notification target that captures structured SQLMesh events in-memory.

    Captured categories:
    - audit_failure (blocking & non-blocking)
    - run_start / run_end / run_failure
    - apply_start / apply_end / apply_failure

    Note: SQLMesh notifier API does not emit "audit success" events.
    """

    type_: t.Literal["capturing"] = Field(alias="type", default="capturing")
    notify_on: t.FrozenSet[NotificationEvent] = frozenset(
        {
            NotificationEvent.AUDIT_FAILURE,
            NotificationEvent.RUN_START,
            NotificationEvent.RUN_END,
            NotificationEvent.RUN_FAILURE,
            NotificationEvent.APPLY_START,
            NotificationEvent.APPLY_END,
            NotificationEvent.APPLY_FAILURE,
        }
    )

    # Private (non-validated, mutable) stores
    _audit_failures: list[dict[str, t.Any]] = PrivateAttr(default_factory=list)
    _run_events: list[dict[str, t.Any]] = PrivateAttr(default_factory=list)
    _apply_events: list[dict[str, t.Any]] = PrivateAttr(default_factory=list)

    # Optional base hook used by default helpers in BaseNotificationTarget
    def send(
        self, _notification_status: NotificationStatus, _msg: str, **_kwargs: t.Any
    ) -> None:  # noqa: D401 - keep signature
        # No outbound side-effect needed; we only capture
        return None

    # ---------------------- Run lifecycle ----------------------
    def notify_run_start(self, environment: str, *_: t.Any, **__: t.Any) -> None:
        self._run_events.append({"event": "run_start", "environment": environment})

    def notify_run_end(self, environment: str, *_: t.Any, **__: t.Any) -> None:
        self._run_events.append({"event": "run_end", "environment": environment})

    def notify_run_failure(self, exc: str, *_: t.Any, **__: t.Any) -> None:
        self._run_events.append({"event": "run_failure", "exception": exc})

    # ---------------------- Apply lifecycle ----------------------
    def notify_apply_start(
        self, environment: str, plan_id: str, *_: t.Any, **__: t.Any
    ) -> None:
        self._apply_events.append(
            {"event": "apply_start", "environment": environment, "plan_id": plan_id}
        )

    def notify_apply_end(
        self, environment: str, plan_id: str, *_: t.Any, **__: t.Any
    ) -> None:
        self._apply_events.append(
            {"event": "apply_end", "environment": environment, "plan_id": plan_id}
        )

    def notify_apply_failure(
        self, environment: str, plan_id: str, exc: str, *_: t.Any, **__: t.Any
    ) -> None:
        self._apply_events.append(
            {
                "event": "apply_failure",
                "environment": environment,
                "plan_id": plan_id,
                "exception": exc,
            }
        )

    # ---------------------- Audits (blocking & non-blocking) ----------------------
    def notify_audit_failure(
        self, audit_error: AuditError, *_: t.Any, **__: t.Any
    ) -> None:
        details = extract_failed_audit_details(audit_error)
        audit_failure_record = {
            "model": details.get("model_name"),
            "audit": details.get("name"),
            "args": details.get("args", {}),
            "count": details.get("count", 0),
            "sql": details.get("sql"),
            "blocking": details.get("blocking", True),
        }
        self._audit_failures.append(audit_failure_record)

    # ---------------------- Accessors ----------------------
    def get_audit_failures(self) -> list[dict[str, t.Any]]:
        return list(self._audit_failures)

    def get_run_events(self) -> list[dict[str, t.Any]]:
        return list(self._run_events)

    def get_apply_events(self) -> list[dict[str, t.Any]]:
        return list(self._apply_events)

    # ---------------------- Test helpers / maintenance ----------------------
    def clear(self) -> None:
        """Clear all captured events. Useful for test isolation."""
        self._audit_failures.clear()
        self._run_events.clear()
        self._apply_events.clear()
