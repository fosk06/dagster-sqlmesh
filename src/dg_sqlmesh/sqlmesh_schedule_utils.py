"""
Utilitaires pour les schedules SQLMesh.
"""

import datetime
from typing import Optional, Tuple, Dict, Any
from dagster import SkipReason, ScheduleEvaluationContext
from sqlmesh.utils import CompletionStatus
from .resource import SQLMeshResource


def should_skip_sqlmesh_run(
    sqlmesh_resource: SQLMeshResource,
    context: ScheduleEvaluationContext,
    environment: Optional[str] = None,
) -> Optional[SkipReason]:
    """
    Détermine si un run SQLMesh doit être ignoré basé sur un dry-run.

    Args:
        sqlmesh_resource: Le SQLMesh resource configuré
        context: Le contexte d'évaluation du schedule Dagster
        environment: L'environnement SQLMesh à utiliser (optionnel)

    Returns:
        SkipReason si le run doit être ignoré, None sinon
    """
    try:
        # Créer un SQLMesh resource temporaire pour le dry-run
        temp_sqlmesh_resource = SQLMeshResource(
            project_dir=sqlmesh_resource.project_dir,
            gateway=sqlmesh_resource.gateway,
            environment=sqlmesh_resource.environment,
            translator=sqlmesh_resource.translator,
            concurrency_limit=sqlmesh_resource.concurrency_limit,
        )

        # Effectuer le dry-run pour vérifier s'il y a des modèles à exécuter
        completion_status, dry_run_summary = temp_sqlmesh_resource.context.dry_run(
            environment=environment or sqlmesh_resource.environment,
            execution_time=context.scheduled_execution_time or datetime.datetime.now(),
        )

        # Vérifier s'il y a des modèles à exécuter
        if completion_status.is_nothing_to_do or dry_run_summary["would_execute"] == 0:
            return SkipReason(
                f"No new data available - nothing to process. Dry-run summary: {dry_run_summary['would_execute']} models would be executed"
            )

        context.log.info(
            f"SQLMesh dry-run completed: {dry_run_summary['would_execute']} models will be executed"
        )

        return None  # Pas de skip, continuer avec le run

    except Exception as e:
        context.log.warning(f"SQLMesh dry-run failed, proceeding with run: {e}")
        return None  # En cas d'erreur, continuer avec le run comme fallback


def get_sqlmesh_dry_run_summary(
    sqlmesh_resource: SQLMeshResource,
    environment: Optional[str] = None,
    execution_time: Optional[datetime.datetime] = None,
) -> Tuple[CompletionStatus, Dict[str, Any]]:
    """
    Obtient un résumé du dry-run SQLMesh.

    Args:
        sqlmesh_resource: Le SQLMesh resource configuré
        environment: L'environnement SQLMesh à utiliser (optionnel)
        execution_time: Le temps d'exécution (optionnel)

    Returns:
        Tuple[CompletionStatus, Dict]: (status, dry_run_summary)
    """
    # Créer un SQLMesh resource temporaire pour le dry-run
    temp_sqlmesh_resource = SQLMeshResource(
        project_dir=sqlmesh_resource.project_dir,
        gateway=sqlmesh_resource.gateway,
        environment=sqlmesh_resource.environment,
        translator=sqlmesh_resource.translator,
        concurrency_limit=sqlmesh_resource.concurrency_limit,
    )

    # Effectuer le dry-run
    completion_status, dry_run_summary = temp_sqlmesh_resource.context.dry_run(
        environment=environment or sqlmesh_resource.environment,
        execution_time=execution_time or datetime.datetime.now(),
    )

    return completion_status, dry_run_summary
