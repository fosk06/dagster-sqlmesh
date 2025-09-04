"""
Tests pour les utilitaires de schedule SQLMesh.
"""

import pytest
import datetime
from unittest.mock import Mock, patch
from dagster import SkipReason, ScheduleEvaluationContext
from sqlmesh.utils import CompletionStatus

from dg_sqlmesh.sqlmesh_schedule_utils import (
    should_skip_sqlmesh_run,
    get_sqlmesh_dry_run_summary,
)
from dg_sqlmesh import SQLMeshResource


class TestSQLMeshScheduleUtils:
    """Tests pour les utilitaires de schedule SQLMesh."""

    def test_should_skip_sqlmesh_run_with_models_to_execute(self, sqlmesh_resource: SQLMeshResource):
        """Test que should_skip_sqlmesh_run retourne None quand il y a des modèles à exécuter."""
        
        # Mock du contexte de schedule
        mock_context = Mock(spec=ScheduleEvaluationContext)
        mock_context.scheduled_execution_time = datetime.datetime.now()
        mock_context.log.info = Mock()
        
        # Mock du SQLMeshResource temporaire et de son dry_run
        with patch('dg_sqlmesh.sqlmesh_schedule_utils.SQLMeshResource') as mock_resource_class:
            mock_temp_resource = Mock()
            mock_temp_resource.context.dry_run.return_value = (
                CompletionStatus.SUCCESS,
                {
                    "would_execute": 3,
                    "successful_models": ["model1", "model2", "model3"],
                    "total_simulated": 3,
                    "would_fail": 0,
                    "failed_models": [],
                    "executions": []
                }
            )
            mock_resource_class.return_value = mock_temp_resource
            
            result = should_skip_sqlmesh_run(sqlmesh_resource, mock_context)
            
            assert result is None
            mock_context.log.info.assert_called_once_with(
                "SQLMesh dry-run completed: 3 models will be executed"
            )

    def test_should_skip_sqlmesh_run_with_no_models(self, sqlmesh_resource: SQLMeshResource):
        """Test que should_skip_sqlmesh_run retourne SkipReason quand il n'y a pas de modèles à exécuter."""
        
        # Mock du contexte de schedule
        mock_context = Mock(spec=ScheduleEvaluationContext)
        mock_context.scheduled_execution_time = datetime.datetime.now()
        
        # Mock du SQLMeshResource temporaire et de son dry_run
        with patch('dg_sqlmesh.sqlmesh_schedule_utils.SQLMeshResource') as mock_resource_class:
            mock_temp_resource = Mock()
            mock_temp_resource.context.dry_run.return_value = (
                CompletionStatus.NOTHING_TO_DO,
                {
                    "would_execute": 0,
                    "successful_models": [],
                    "total_simulated": 0,
                    "would_fail": 0,
                    "failed_models": [],
                    "executions": []
                }
            )
            mock_resource_class.return_value = mock_temp_resource
            
            result = should_skip_sqlmesh_run(sqlmesh_resource, mock_context)
            
            assert isinstance(result, SkipReason)
            assert "No new data available - nothing to process" in str(result)

    def test_should_skip_sqlmesh_run_with_exception(self, sqlmesh_resource: SQLMeshResource):
        """Test que should_skip_sqlmesh_run gère les exceptions et continue."""
        
        # Mock du contexte de schedule
        mock_context = Mock(spec=ScheduleEvaluationContext)
        mock_context.scheduled_execution_time = datetime.datetime.now()
        mock_context.log.warning = Mock()
        
        # Mock du SQLMeshResource temporaire et de son dry_run qui lève une exception
        with patch('dg_sqlmesh.sqlmesh_schedule_utils.SQLMeshResource') as mock_resource_class:
            mock_temp_resource = Mock()
            mock_temp_resource.context.dry_run.side_effect = Exception("Test error")
            mock_resource_class.return_value = mock_temp_resource
            
            result = should_skip_sqlmesh_run(sqlmesh_resource, mock_context)
            
            assert result is None  # Continue avec le run
            mock_context.log.warning.assert_called_once_with(
                "SQLMesh dry-run failed, proceeding with run: Test error"
            )

    def test_get_sqlmesh_dry_run_summary(self, sqlmesh_resource: SQLMeshResource):
        """Test que get_sqlmesh_dry_run_summary fonctionne correctement."""
        
        execution_time = datetime.datetime.now()
        
        # Mock du SQLMeshResource temporaire et de son dry_run
        with patch('dg_sqlmesh.sqlmesh_schedule_utils.SQLMeshResource') as mock_resource_class:
            mock_temp_resource = Mock()
            mock_temp_resource.context.dry_run.return_value = (
                CompletionStatus.SUCCESS,
                {
                    "would_execute": 2,
                    "successful_models": ["model1", "model2"],
                    "total_simulated": 2,
                    "would_fail": 0,
                    "failed_models": [],
                    "executions": []
                }
            )
            mock_resource_class.return_value = mock_temp_resource
            
            completion_status, dry_run_summary = get_sqlmesh_dry_run_summary(
                sqlmesh_resource,
                environment="dev",
                execution_time=execution_time
            )
            
            assert completion_status == CompletionStatus.SUCCESS
            assert dry_run_summary["would_execute"] == 2
            assert dry_run_summary["successful_models"] == ["model1", "model2"]
            
            # Vérifier que dry_run a été appelé avec les bons paramètres
            mock_temp_resource.context.dry_run.assert_called_once_with(
                environment="dev",
                execution_time=execution_time
            )

    def test_get_sqlmesh_dry_run_summary_defaults(self, sqlmesh_resource: SQLMeshResource):
        """Test que get_sqlmesh_dry_run_summary utilise les valeurs par défaut."""
        
        # Mock du SQLMeshResource temporaire et de son dry_run
        with patch('dg_sqlmesh.sqlmesh_schedule_utils.SQLMeshResource') as mock_resource_class:
            mock_temp_resource = Mock()
            mock_temp_resource.context.dry_run.return_value = (
                CompletionStatus.SUCCESS,
                {"would_execute": 1, "successful_models": ["model1"]}
            )
            mock_resource_class.return_value = mock_temp_resource
            
            completion_status, dry_run_summary = get_sqlmesh_dry_run_summary(sqlmesh_resource)
            
            assert completion_status == CompletionStatus.SUCCESS
            
            # Vérifier que dry_run a été appelé avec les valeurs par défaut
            mock_temp_resource.context.dry_run.assert_called_once()
            call_args = mock_temp_resource.context.dry_run.call_args
            assert call_args[1]["environment"] == sqlmesh_resource.environment
            assert call_args[1]["execution_time"] is not None  # datetime.now()
