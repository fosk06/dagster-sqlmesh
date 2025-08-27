"""
Console ultra-simple qui ne track QUE les modèles skippés et exécutés.
Rien d'autre.
"""

import typing as t
import uuid
from sqlmesh.core.console import Console
from sqlmesh.core.snapshot.definition import (
    Snapshot,
    SnapshotInfoLike,
    SnapshotTableInfo,
)
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.plan.definition import EvaluatablePlan
from sqlmesh.core.test.result import ModelTextTestResult
from sqlmesh.core.linter.definition import RuleViolation
from sqlmesh.core.model import Model
from sqlmesh.core.table_diff import TableDiff
from sqlmesh.core.environment import EnvironmentSummary
from sqlmesh.utils.concurrency import NodeExecutionFailedError
from sqlmesh.core.snapshot.definition import SnapshotId
from contextlib import contextmanager


class SimpleRunTracker(Console):
    """
    Console minimaliste qui track SEULEMENT :
    - Les modèles qui ont été exécutés (run)
    - Les modèles qui ont été skippés
    """

    def __init__(self):
        self.run_models: t.Set[str] = set()
        self.skipped_models: t.Set[str] = set()

    def get_results(self) -> t.Dict[str, t.Any]:
        """Retourne les résultats du tracking."""
        results = {
            "run_models": list(self.run_models),
            "skipped_models": list(self.skipped_models),
            "total_run": len(self.run_models),
            "total_skipped": len(self.skipped_models),
        }
        return results

    def clear(self):
        """Remet à zéro le tracking."""
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
        """MODÈLE EXÉCUTÉ - juste ajouter le nom."""
        self.run_models.add(snapshot.name)

    def log_skipped_models(self, snapshot_names: t.Set[str]) -> None:
        self.skipped_models.update(snapshot_names)

    def start_plan_evaluation(self, plan: EvaluatablePlan) -> None:
        pass

    def stop_plan_evaluation(self) -> None:
        pass

    def start_evaluation_progress(
        self,
        batched_intervals: t.Dict[Snapshot, t.Any],
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
        audit_only: bool = False,
    ) -> None:
        pass

    def start_snapshot_evaluation_progress(
        self, snapshot: Snapshot, audit_only: bool = False
    ) -> None:
        pass

    def stop_evaluation_progress(self, success: bool = True) -> None:
        pass

    def start_signal_progress(
        self,
        snapshot: Snapshot,
        default_catalog: t.Optional[str],
        environment_naming_info: EnvironmentNamingInfo,
    ) -> None:
        pass

    def update_signal_progress(
        self,
        snapshot: Snapshot,
        signal_name: str,
        signal_idx: int,
        total_signals: int,
        ready_intervals: t.Any,
        check_intervals: t.Any,
        duration: float,
    ) -> None:
        pass

    def stop_signal_progress(self) -> None:
        pass

    def start_creation_progress(
        self,
        snapshots: t.List[Snapshot],
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        pass

    def update_creation_progress(self, snapshot: SnapshotInfoLike) -> None:
        pass

    def stop_creation_progress(self, success: bool = True) -> None:
        pass

    def start_cleanup(self, ignore_ttl: bool) -> bool:
        return True

    def update_cleanup_progress(self, object_name: str) -> None:
        pass

    def stop_cleanup(self, success: bool = True) -> None:
        pass

    def start_promotion_progress(
        self,
        snapshots: t.List[SnapshotTableInfo],
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        pass

    def update_promotion_progress(
        self, snapshot: SnapshotInfoLike, promoted: bool
    ) -> None:
        pass

    def stop_promotion_progress(self, success: bool = True) -> None:
        pass

    def start_snapshot_migration_progress(self, _total_tasks: int) -> None:
        pass

    def update_snapshot_migration_progress(self, migration_status: str) -> None:
        pass

    def stop_snapshot_migration_progress(self, success: bool = True) -> None:
        pass

    def start_migration_progress(self, total_tasks: int) -> None:
        pass

    def update_migration_progress(self, migration_status: str) -> None:
        pass

    def stop_migration_progress(self, success: bool = True) -> None:
        pass

    def start_environment_migration_progress(self, total_tasks: int) -> None:
        pass

    def update_environment_migration_progress(self, migration_status: str) -> None:
        pass

    def stop_environment_migration_progress(self, success: bool = True) -> None:
        pass

    def log_status_update(self, message: str) -> None:
        pass

    def loading_start(self, message: t.Optional[str] = None) -> uuid.UUID:
        return uuid.uuid4()

    def loading_stop(self, id: uuid.UUID) -> None:
        pass

    def log_error(self, message: str, *args: t.Any, **kwargs: t.Any) -> None:
        pass

    def log_success(self, message: str, *args: t.Any, **kwargs: t.Any) -> None:
        pass

    def log_destructive_change(
        self,
        snapshot_name: str,
        alter_operations: t.List[t.Any],
        added_columns: t.Set[str],
        removed_columns: t.Set[str],
    ) -> None:
        pass

    def log_failed_models(self, errors: t.List[NodeExecutionFailedError]) -> None:
        pass  # On ignore les fails

    def log_test_results(
        self, result: ModelTextTestResult, target_dialect: str
    ) -> None:
        pass

    def show_linter_violations(
        self, violations: t.List[RuleViolation], model: Model, is_error: bool = False
    ) -> None:
        pass

    def start_state_export(
        self, total_versions: int, total_snapshots: int, total_environments: int
    ) -> None:
        pass

    def update_state_export_progress(
        self,
        total_versions: int,
        versions_exported: int,
        total_snapshots: int,
        snapshots_exported: int,
        total_environments: int,
        environments_exported: int,
    ) -> None:
        pass

    def stop_state_export(self, success: bool = True) -> None:
        pass

    def start_state_import(
        self,
        total_versions: int,
        total_snapshots: int,
        total_environments: int,
        total_plan_dags: int = 0,
    ) -> None:
        pass

    def update_state_import_progress(
        self,
        total_versions: int,
        versions_imported: int,
        total_snapshots: int,
        snapshots_imported: int,
        total_environments: int,
        environments_exported: int,
        total_plan_dags: int = 0,
        plan_dags_imported: int = 0,
    ) -> None:
        pass

    def stop_state_import(self, success: bool = True) -> None:
        pass

    def start_destroy(
        self,
        snapshot_ids: t.Set[SnapshotId],
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        pass

    def stop_destroy(self, success: bool = True) -> None:
        pass

    def print_environments(
        self, environments_summary: t.List[EnvironmentSummary]
    ) -> None:
        pass

    def show_environment_difference_summary(
        self,
        name: str,
        from_environment_name: str,
        to_environment_name: str,
        added: t.Set[str],
        removed_environment_naming_info: EnvironmentNamingInfo,
        removed: t.Set[str],
        modified_snapshots: t.Dict[str, t.Tuple[SnapshotTableInfo, SnapshotTableInfo]],
    ) -> None:
        pass

    def show_table_diff(
        self,
        table_diff: TableDiff,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
        snapshots: t.Dict[str, Snapshot],
        tables: t.List[str],
    ) -> None:
        pass

    def update_table_diff_progress(self, model: str) -> None:
        pass

    def start_table_diff_progress(self, models_to_diff: int) -> None:
        pass

    def start_table_diff_model_progress(self, model: str) -> None:
        pass

    def stop_table_diff_progress(self, success: bool = True) -> None:
        pass

    def log_migration_status(self, message: str) -> None:
        pass

    def log_warning(self, message: str) -> None:
        pass

    def plan(
        self,
        plan_builder: t.Any,  # type: ignore[reportUnknownArgumentType]
        auto_apply: bool,
        default_catalog: t.Optional[str],
        no_diff: bool = False,
        no_prompts: bool = False,
    ) -> None:
        pass

    def show_intervals(self, intervals: t.Any) -> None:
        pass

    def show_model_difference_summary(self, model_diff: t.Any) -> None:
        pass

    def show_row_diff(self, row_diff: t.Any) -> None:
        pass

    def show_schema_diff(self, schema_diff: t.Any) -> None:
        pass

    def show_sql(self, sql: str) -> None:
        pass

    def show_table_diff_details(self, table_diff: t.Any) -> None:
        pass

    def show_table_diff_summary(self, table_diff: t.Any) -> None:
        pass

    def start_env_migration_progress(self, total_tasks: int) -> None:
        pass

    def stop_env_migration_progress(self, success: bool = True) -> None:
        pass

    def update_env_migration_progress(self, migration_status: str) -> None:
        pass


@contextmanager
def sqlmesh_run_tracker(sqlmesh_context):
    """
    Context manager pour tracker les modèles exécutés vs skippés pendant sqlmesh run.

    Args:
        sqlmesh_context: Le contexte SQLMesh dans lequel injecter notre tracker

    Usage:
        with sqlmesh_run_tracker(sqlmesh.context) as tracker:
            # SQLMesh run ici
            plan = sqlmesh.materialize_assets_threaded(...)

            # Récupérer les résultats
            results = tracker.get_results()
            skipped_models = results['skipped_models']
    """
    # Créer notre tracker
    tracker = SimpleRunTracker()

    # Sauvegarder la console actuelle du contexte SQLMesh
    original_console = sqlmesh_context.console

    # Injecter notre tracker dans le contexte SQLMesh
    sqlmesh_context.console = tracker

    try:
        yield tracker  # Donner accès au tracker
    finally:
        # TOUJOURS restaurer la console originale du contexte SQLMesh
        sqlmesh_context.console = original_console
        # Cleanup optionnel
        tracker.clear()
