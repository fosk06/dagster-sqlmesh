"""
Ultra-simple console that tracks ONLY executed and skipped models.
Nothing else.
"""

import typing as t
from sqlmesh.core.console import NoopConsole
from sqlmesh.core.snapshot.definition import Snapshot, SnapshotId
from contextlib import contextmanager


class SimpleRunTracker(NoopConsole):
    """
    Minimal console that tracks ONLY:
    - Models that have been executed (run)  
    - Models that have been skipped
    """

    def __init__(self):
        self.run_models: t.Set[str] = set()
        self.skipped_models: t.Set[str] = set()

    def get_results(self) -> t.Dict[str, t.Any]:
        """Returns tracking results."""
        results = {
            "run_models": list(self.run_models),
            "skipped_models": list(self.skipped_models),
            "total_run": len(self.run_models),
            "total_skipped": len(self.skipped_models),
        }
        return results

    def clear(self):
        """Reset tracking."""
        self.run_models.clear()
        self.skipped_models.clear()

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
        """MODEL EXECUTED - just add the name."""
        self.run_models.add(snapshot.name)

    def log_skipped_models(self, snapshot_names: t.Set[str]) -> None:
        """MODELS SKIPPED - just add the names."""
        self.skipped_models.update(snapshot_names)


@contextmanager
def sqlmesh_run_tracker(sqlmesh_context):
    """
    Context manager to track executed vs skipped models during SQLMesh run.

    Args:
        sqlmesh_context: The SQLMesh context in which to inject our tracker

    Usage:
        with sqlmesh_run_tracker(sqlmesh.context) as tracker:
            # SQLMesh run here
            plan = sqlmesh.materialize_assets_threaded(...)

            # Get results
            results = tracker.get_results()
            skipped_models = results['skipped_models']
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
