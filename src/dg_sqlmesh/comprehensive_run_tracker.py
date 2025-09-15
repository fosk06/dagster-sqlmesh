"""
Comprehensive console that tracks model execution states.
Handles SQLMesh batch execution and early exit scenarios.
"""

import typing as t
from sqlmesh.core.console import NoopConsole, set_console
from sqlmesh.core.snapshot.definition import Snapshot, SnapshotId
from sqlmesh.core.snapshot.definition import Interval
from sqlmesh.core.snapshot.execution_tracker import QueryExecutionStats


class ComprehensiveRunTracker(NoopConsole):
    """
    Comprehensive console that tracks all model execution states.
    Handles SQLMesh batch execution and early exit scenarios.
    """

    def __init__(self, logger=None):
        self.executed_models: t.Set[str] = set()  # Models that completed successfully
        self.failed_models: t.Set[str] = set()    # Models that failed (audit failures)
        self.requested_models: t.Set[str] = set() # All models that were requested for execution
        self.logger = logger

    def get_executed_models(self) -> t.List[str]:
        """Returns list of successfully executed model names."""
        return list(self.executed_models)

    def get_failed_models(self) -> t.List[str]:
        """Returns list of failed model names."""
        return list(self.failed_models)

    def get_requested_models(self) -> t.List[str]:
        """Returns list of all requested model names."""
        return list(self.requested_models)


    def clear(self):
        """Reset tracking."""
        self.executed_models.clear()
        self.failed_models.clear()
        self.requested_models.clear()

    def set_requested_models(self, model_names: t.List[str]) -> None:
        """Set the list of models that were requested for execution."""
        self.requested_models = set(model_names)

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
        if self.logger:
            self.logger.debug(f"✅ Executed model: {snapshot.name}")
            self.logger.debug(f"✅ num_audits_passed : {num_audits_passed}")
            self.logger.debug(f"✅ num_audits_failed : {num_audits_failed}")
        else:
            print(f"✅ Executed model: {snapshot.name}")
            print(f"✅ num_audits_passed : {num_audits_passed}")
            print(f"✅ num_audits_failed : {num_audits_failed}")
        self.executed_models.add(snapshot.name)

    def add_failed_model(self, model_name: str) -> None:
        """Add a model that failed (e.g., due to audit failure)."""
        if self.logger:
            self.logger.debug(f"❌ Failed model: {model_name}")
        else:
            print(f"❌ Failed model: {model_name}")
        self.failed_models.add(model_name)


# Global tracker instance - comprehensive and reliable
_GLOBAL_TRACKER = ComprehensiveRunTracker()


def get_global_tracker() -> ComprehensiveRunTracker:
    """Get the global tracker instance."""
    return _GLOBAL_TRACKER


def reset_global_tracker() -> None:
    """Reset the global tracker."""
    _GLOBAL_TRACKER.clear()


def setup_global_tracker(logger=None) -> None:
    """Setup the global tracker using set_console (recommended method)."""
    global _GLOBAL_TRACKER
    _GLOBAL_TRACKER = ComprehensiveRunTracker(logger=logger)
    set_console(_GLOBAL_TRACKER)