"""
Console ultra-simple qui ne track QUE les modèles skippés et exécutés.
Rien d'autre.
"""

import typing as t
import uuid
from sqlmesh.core.console import Console
from sqlmesh.core.snapshot.definition import Snapshot, SnapshotInfoLike, SnapshotTableInfo
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
        interval: t.Any,  # On s'en fiche du type exact
        _batch_idx: int,
        _duration_ms: t.Optional[int],
        _num_audits_passed: int,
        _num_audits_failed: int,
        _audit_only: bool = False,
        _auto_restatement_triggers: t.Optional[t.List[SnapshotId]] = None,
    ) -> None:
        """MODÈLE EXÉCUTÉ - juste ajouter le nom."""
        self.run_models.add(snapshot.name)
    
    def log_skipped_models(_self, _snapshot_names: t.Set[str]) -> None: pass
    def start_plan_evaluation(_self, _plan: EvaluatablePlan) -> None: pass
    def stop_plan_evaluation(_self) -> None: pass
    def start_evaluation_progress(_self, _batched_intervals: t.Dict[Snapshot, t.Any], _environment_naming_info: EnvironmentNamingInfo, _default_catalog: t.Optional[str], _audit_only: bool = False) -> None: pass
    def start_snapshot_evaluation_progress(_self, _snapshot: Snapshot, _audit_only: bool = False) -> None: pass
    def stop_evaluation_progress(_self, _success: bool = True) -> None: pass
    def start_signal_progress(_self, _snapshot: Snapshot, _default_catalog: t.Optional[str], _environment_naming_info: EnvironmentNamingInfo) -> None: pass
    def update_signal_progress(_self, _snapshot: Snapshot, _signal_name: str, _signal_idx: int, _total_signals: int, _ready_intervals: t.Any, _check_intervals: t.Any, _duration: float) -> None: pass
    def stop_signal_progress(_self) -> None: pass
    def start_creation_progress(_self, _snapshots: t.List[Snapshot], _environment_naming_info: EnvironmentNamingInfo, _default_catalog: t.Optional[str]) -> None: pass
    def update_creation_progress(_self, _snapshot: SnapshotInfoLike) -> None: pass
    def stop_creation_progress(_self, _success: bool = True) -> None: pass
    def start_cleanup(_self, _ignore_ttl: bool) -> bool: return True
    def update_cleanup_progress(_self, _object_name: str) -> None: pass
    def stop_cleanup(_self, _success: bool = True) -> None: pass
    def start_promotion_progress(_self, _snapshots: t.List[SnapshotTableInfo], _environment_naming_info: EnvironmentNamingInfo, _default_catalog: t.Optional[str]) -> None: pass
    def update_promotion_progress(_self, _snapshot: SnapshotInfoLike, _promoted: bool) -> None: pass
    def stop_promotion_progress(_self, _success: bool = True) -> None: pass
    def start_snapshot_migration_progress(_self, _total_tasks: int) -> None: pass
    def update_snapshot_migration_progress(_self, _migration_status: str) -> None: pass
    def stop_snapshot_migration_progress(_self, _success: bool = True) -> None: pass
    def start_migration_progress(_self, _total_tasks: int) -> None: pass
    def update_migration_progress(_self, _migration_status: str) -> None: pass
    def stop_migration_progress(_self, _success: bool = True) -> None: pass
    def start_environment_migration_progress(_self, _total_tasks: int) -> None: pass
    def update_environment_migration_progress(_self, _migration_status: str) -> None: pass
    def stop_environment_migration_progress(_self, _success: bool = True) -> None: pass
    def log_status_update(_self, _message: str) -> None: pass
    def loading_start(_self, _message: t.Optional[str] = None) -> uuid.UUID: return uuid.uuid4()
    def loading_stop(_self, _id: uuid.UUID) -> None: pass
    def log_error(_self, _message: str, *_args: t.Any, **_kwargs: t.Any) -> None: pass
    def log_success(_self, _message: str, *_args: t.Any, **_kwargs: t.Any) -> None: pass
    def log_destructive_change(_self, _snapshot_name: str, _alter_operations: t.List[t.Any], _added_columns: t.Set[str], _removed_columns: t.Set[str]) -> None: pass
    def log_failed_models(_self, _errors: t.List[NodeExecutionFailedError]) -> None: pass  # On ignore les fails
    def log_test_results(_self, _result: ModelTextTestResult, _target_dialect: str) -> None: pass
    def show_linter_violations(_self, _violations: t.List[RuleViolation], _model: Model, _is_error: bool = False) -> None: pass
    def start_state_export(_self, _total_versions: int, _total_snapshots: int, _total_environments: int) -> None: pass
    def update_state_export_progress(_self, _total_versions: int, _versions_exported: int, _total_snapshots: int, _snapshots_exported: int, _total_environments: int, _environments_exported: int) -> None: pass
    def stop_state_export(_self, _success: bool = True) -> None: pass
    def start_state_import(_self, _total_versions: int, _total_snapshots: int, _total_environments: int, _total_plan_dags: int = 0) -> None: pass
    def update_state_import_progress(_self, _total_versions: int, _versions_imported: int, _total_snapshots: int, _snapshots_imported: int, _total_environments: int, _environments_exported: int, _total_plan_dags: int = 0, _plan_dags_imported: int = 0) -> None: pass
    def stop_state_import(_self, _success: bool = True) -> None: pass
    def start_destroy(_self, _snapshot_ids: t.Set[SnapshotId], _environment_naming_info: EnvironmentNamingInfo, _default_catalog: t.Optional[str]) -> None: pass
    def stop_destroy(_self, _success: bool = True) -> None: pass
    def print_environments(_self, _environments_summary: t.List[EnvironmentSummary]) -> None: pass
    def show_environment_difference_summary(_self, _name: str, _from_environment_name: str, _to_environment_name: str, _added: t.Set[str], _removed_environment_naming_info: EnvironmentNamingInfo, _removed: t.Set[str], _modified_snapshots: t.Dict[str, t.Tuple[SnapshotTableInfo, SnapshotTableInfo]]) -> None: pass
    def show_table_diff(_self, _table_diff: TableDiff, _environment_naming_info: EnvironmentNamingInfo, _default_catalog: t.Optional[str], _snapshots: t.Dict[str, Snapshot], _tables: t.List[str]) -> None: pass
    def update_table_diff_progress(_self, _model: str) -> None: pass
    def start_table_diff_progress(_self, _models_to_diff: int) -> None: pass
    def start_table_diff_model_progress(_self, _model: str) -> None: pass
    def stop_table_diff_progress(_self, _success: bool = True) -> None: pass
    def log_migration_status(_self, _message: str) -> None: pass
    def log_warning(_self, _message: str) -> None: pass
    def plan(
        _self,
        _plan_builder: t.Any,
        _auto_apply: bool,
        _default_catalog: t.Optional[str],
        _no_diff: bool = False,
        _no_prompts: bool = False,
    ) -> None:
        pass
    def show_intervals(_self, _intervals: t.Any) -> None: pass
    def show_model_difference_summary(_self, _model_diff: t.Any) -> None: pass
    def show_row_diff(_self, _row_diff: t.Any) -> None: pass
    def show_schema_diff(_self, _schema_diff: t.Any) -> None: pass
    def show_sql(_self, _sql: str) -> None: pass
    def show_table_diff_details(_self, _table_diff: t.Any) -> None: pass
    def show_table_diff_summary(_self, _table_diff: t.Any) -> None: pass
    def start_env_migration_progress(_self, _total_tasks: int) -> None: pass
    def stop_env_migration_progress(_self, _success: bool = True) -> None: pass
    def update_env_migration_progress(_self, _migration_status: str) -> None: pass


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
