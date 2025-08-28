"""
Ultra-simple console that tracks ONLY executed models.
Skipped models are deduced externally by: requested_models - executed_models
"""

import typing as t
from sqlmesh.core.console import NoopConsole
from sqlmesh.core.snapshot.definition import Snapshot, SnapshotId
from contextlib import contextmanager


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
        interval: t.Any,
        _batch_idx: int,
        _duration_ms: t.Optional[int],
        _num_audits_passed: int,
        _num_audits_failed: int,
        _audit_only: bool = False,
        _auto_restatement_triggers: t.Optional[t.List[SnapshotId]] = None,
    ) -> None:
        """Track executed model."""
        self.executed_models.add(snapshot.name)


@contextmanager
def sqlmesh_run_tracker(sqlmesh_context):
    """
    Context manager to track executed models during SQLMesh run.
    Skipped models are deduced externally by: requested_models - executed_models

    Args:
        sqlmesh_context: The SQLMesh context in which to inject our tracker

    Usage:
        with sqlmesh_run_tracker(sqlmesh.context) as tracker:
            # SQLMesh run here
            plan = sqlmesh.materialize_assets_threaded(...)

            # Get executed models
            executed_models = tracker.get_executed_models()
            skipped_models = list(set(requested_models) - set(executed_models))
    """
    # Create our tracker
    tracker = SimpleRunTracker()

    # Save current console from SQLMesh context
    original_console = sqlmesh_context.console

    # Inject our tracker into SQLMesh context
    sqlmesh_context.console = tracker

    try:
        yield tracker  # Give access to tracker
    finally:
        # ALWAYS restore original console from SQLMesh context
        sqlmesh_context.console = original_console
        # Optional cleanup
        tracker.clear()
