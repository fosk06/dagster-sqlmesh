"""
Tests for SQLMesh schedule utilities.
"""

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
    """Tests for SQLMesh schedule utilities."""

    def test_should_skip_sqlmesh_run_with_models_to_execute(
        self, sqlmesh_resource: SQLMeshResource
    ):
        """Test that should_skip_sqlmesh_run returns None when there are models to execute."""

        # Mock the schedule context
        mock_context = Mock(spec=ScheduleEvaluationContext)
        mock_context.scheduled_execution_time = datetime.datetime.now()
        mock_context.log.info = Mock()

        # Mock the temporary SQLMeshResource and its dry_run
        with patch(
            "dg_sqlmesh.sqlmesh_schedule_utils.SQLMeshResource"
        ) as mock_resource_class:
            mock_temp_resource = Mock()
            mock_temp_resource.context.dry_run.return_value = (
                CompletionStatus.SUCCESS,
                {
                    "would_execute": 3,
                    "successful_models": ["model1", "model2", "model3"],
                    "total_simulated": 3,
                    "would_fail": 0,
                    "failed_models": [],
                    "executions": [],
                },
            )
            mock_resource_class.return_value = mock_temp_resource

            result = should_skip_sqlmesh_run(sqlmesh_resource, mock_context)

            assert result is None
            mock_context.log.info.assert_called_once_with(
                "SQLMesh dry-run completed: 3 models will be executed"
            )

    def test_should_skip_sqlmesh_run_with_no_models(
        self, sqlmesh_resource: SQLMeshResource
    ):
        """Test that should_skip_sqlmesh_run returns SkipReason when there are no models to execute."""

        # Mock the schedule context
        mock_context = Mock(spec=ScheduleEvaluationContext)
        mock_context.scheduled_execution_time = datetime.datetime.now()

        # Mock the temporary SQLMeshResource and its dry_run
        with patch(
            "dg_sqlmesh.sqlmesh_schedule_utils.SQLMeshResource"
        ) as mock_resource_class:
            mock_temp_resource = Mock()
            mock_temp_resource.context.dry_run.return_value = (
                CompletionStatus.NOTHING_TO_DO,
                {
                    "would_execute": 0,
                    "successful_models": [],
                    "total_simulated": 0,
                    "would_fail": 0,
                    "failed_models": [],
                    "executions": [],
                },
            )
            mock_resource_class.return_value = mock_temp_resource

            result = should_skip_sqlmesh_run(sqlmesh_resource, mock_context)

            assert isinstance(result, SkipReason)
            assert "No new data available - nothing to process" in str(result)

    def test_should_skip_sqlmesh_run_with_exception(
        self, sqlmesh_resource: SQLMeshResource
    ):
        """Test that should_skip_sqlmesh_run handles exceptions and continues."""

        # Mock the schedule context
        mock_context = Mock(spec=ScheduleEvaluationContext)
        mock_context.scheduled_execution_time = datetime.datetime.now()
        mock_context.log.warning = Mock()

        # Mock the temporary SQLMeshResource and its dry_run that raises an exception
        with patch(
            "dg_sqlmesh.sqlmesh_schedule_utils.SQLMeshResource"
        ) as mock_resource_class:
            mock_temp_resource = Mock()
            mock_temp_resource.context.dry_run.side_effect = Exception("Test error")
            mock_resource_class.return_value = mock_temp_resource

            result = should_skip_sqlmesh_run(sqlmesh_resource, mock_context)

            assert result is None  # Continue with the run
            mock_context.log.warning.assert_called_once_with(
                "SQLMesh dry-run failed, proceeding with run: Test error"
            )

    def test_get_sqlmesh_dry_run_summary(self, sqlmesh_resource: SQLMeshResource):
        """Test that get_sqlmesh_dry_run_summary works correctly."""

        execution_time = datetime.datetime.now()

        # Mock the temporary SQLMeshResource and its dry_run
        with patch(
            "dg_sqlmesh.sqlmesh_schedule_utils.SQLMeshResource"
        ) as mock_resource_class:
            mock_temp_resource = Mock()
            mock_temp_resource.context.dry_run.return_value = (
                CompletionStatus.SUCCESS,
                {
                    "would_execute": 2,
                    "successful_models": ["model1", "model2"],
                    "total_simulated": 2,
                    "would_fail": 0,
                    "failed_models": [],
                    "executions": [],
                },
            )
            mock_resource_class.return_value = mock_temp_resource

            completion_status, dry_run_summary = get_sqlmesh_dry_run_summary(
                sqlmesh_resource, environment="dev", execution_time=execution_time
            )

            assert completion_status == CompletionStatus.SUCCESS
            assert dry_run_summary["would_execute"] == 2
            assert dry_run_summary["successful_models"] == ["model1", "model2"]

            # Verify that dry_run was called with the correct parameters
            mock_temp_resource.context.dry_run.assert_called_once_with(
                environment="dev", execution_time=execution_time
            )

    def test_get_sqlmesh_dry_run_summary_defaults(
        self, sqlmesh_resource: SQLMeshResource
    ):
        """Test that get_sqlmesh_dry_run_summary uses default values."""

        # Mock the temporary SQLMeshResource and its dry_run
        with patch(
            "dg_sqlmesh.sqlmesh_schedule_utils.SQLMeshResource"
        ) as mock_resource_class:
            mock_temp_resource = Mock()
            mock_temp_resource.context.dry_run.return_value = (
                CompletionStatus.SUCCESS,
                {"would_execute": 1, "successful_models": ["model1"]},
            )
            mock_resource_class.return_value = mock_temp_resource

            completion_status, dry_run_summary = get_sqlmesh_dry_run_summary(
                sqlmesh_resource
            )

            assert completion_status == CompletionStatus.SUCCESS

            # Verify that dry_run was called with default values
            mock_temp_resource.context.dry_run.assert_called_once()
            call_args = mock_temp_resource.context.dry_run.call_args
            assert call_args[1]["environment"] == sqlmesh_resource.environment
            assert call_args[1]["execution_time"] is not None  # datetime.now()
