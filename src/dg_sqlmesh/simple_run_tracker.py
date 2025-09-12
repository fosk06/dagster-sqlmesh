"""
Ultra-simple console that tracks ONLY executed models.
Skipped models are deduced externally by: requested_models - executed_models
"""

import typing as t
from sqlmesh.core.console import NoopConsole
from sqlmesh.core.snapshot.definition import Snapshot, SnapshotId
from sqlmesh.core.snapshot.definition import Interval
from sqlmesh.core.snapshot.execution_tracker import QueryExecutionStats


class SimpleRunTracker(NoopConsole):
    """
    Minimal console that tracks ONLY executed models.
    Skipped models are deduced externally by: requested_models - executed_models
    """

    def __init__(self):
        self.executed_models: t.Set[str] = set()

    def get_executed_models(self) -> t.List[str]:
        """Returns list of executed model names."""
        return list(self.executed_models)

    def clear(self):
        """Reset tracking."""
        self.executed_models.clear()

    def update_snapshot_evaluation_progress(
        self,
        snapshot: Snapshot,
        interval: Interval,
        batch_idx: int,
        duration_ms: t.Optional[int],
        num_audits_passed: int,
        num_audits_failed: int,
        audit_only: bool = False,
        execution_stats: t.Optional[QueryExecutionStats] = None,
        auto_restatement_triggers: t.Optional[t.List[SnapshotId]] = None,
    ) -> None:
        """Track executed model."""
        print(f"✅ Executed model: {snapshot.name}")
        self.executed_models.add(snapshot.name)


# Global tracker instance - simple and reliable
_GLOBAL_TRACKER = SimpleRunTracker()


def get_global_tracker() -> SimpleRunTracker:
    """Get the global tracker instance."""
    return _GLOBAL_TRACKER


def reset_global_tracker() -> None:
    """Reset the global tracker."""
    _GLOBAL_TRACKER.clear()