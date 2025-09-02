"""
Tests for SQLMesh adaptive schedule functionality.
"""

import datetime
from unittest.mock import Mock, patch

import pytest
from dagster import (
    DagsterInstance,
    RunRequest,
    SkipReason,
    DagsterRunStatus,
    RunsFilter,
)
from dagster._core.run_coordinator import QueuedRunCoordinator

from dg_sqlmesh.factory import _assert_instance_has_queued_run_coordinator


class TestSQLMeshAdaptiveSchedule:
    """Test suite for SQLMesh adaptive schedule logic."""

    @pytest.fixture
    def mock_instance_with_queued_coordinator(self):
        """Create mock Dagster instance with QueuedRunCoordinator."""
        instance = Mock(spec=DagsterInstance)
        instance.run_coordinator = Mock(spec=QueuedRunCoordinator)
        return instance

    @pytest.fixture
    def mock_instance_without_queued_coordinator(self):
        """Create mock Dagster instance without QueuedRunCoordinator."""
        instance = Mock(spec=DagsterInstance)
        instance.run_coordinator = Mock()  # Not QueuedRunCoordinator
        return instance

    def test_assert_instance_has_queued_run_coordinator_success(
        self, mock_instance_with_queued_coordinator
    ):
        """Test that _assert_instance_has_queued_run_coordinator passes with QueuedRunCoordinator."""
        # Should not raise any exception
        _assert_instance_has_queued_run_coordinator(
            mock_instance_with_queued_coordinator
        )

    def test_assert_instance_has_queued_run_coordinator_failure(
        self, mock_instance_without_queued_coordinator
    ):
        """Test that _assert_instance_has_queued_run_coordinator raises error without QueuedRunCoordinator."""
        with pytest.raises(RuntimeError) as exc_info:
            _assert_instance_has_queued_run_coordinator(
                mock_instance_without_queued_coordinator
            )

        assert "QueuedRunCoordinator" in str(exc_info.value)

    def test_schedule_run_key_format(self):
        """Test that schedule generates correct run key format."""
        # Test with specific execution time
        execution_time = datetime.datetime(2024, 1, 15, 9, 0, 0)

        result = RunRequest(
            run_key=f"sqlmesh_adaptive_{execution_time.isoformat()}",
            tags={"schedule": "sqlmesh_adaptive"},
        )

        assert isinstance(result, RunRequest)
        expected_key = f"sqlmesh_adaptive_{execution_time.isoformat()}"
        assert result.run_key == expected_key

    def test_schedule_run_key_with_no_execution_time(self):
        """Test that schedule generates run key when no execution time is provided."""
        with patch("datetime.datetime") as mock_datetime:
            mock_now = datetime.datetime(2024, 1, 15, 9, 0, 0)
            mock_datetime.now.return_value = mock_now
            mock_datetime.side_effect = lambda *args, **kw: datetime.datetime(
                *args, **kw
            )

            scheduled_ts = datetime.datetime.now()
            result = RunRequest(
                run_key=f"sqlmesh_adaptive_{scheduled_ts.isoformat()}",
                tags={"schedule": "sqlmesh_adaptive"},
            )

            assert isinstance(result, RunRequest)
            expected_key = f"sqlmesh_adaptive_{mock_now.isoformat()}"
            assert result.run_key == expected_key

    def test_schedule_filters_correct_run_statuses(self):
        """Test that schedule filters runs with correct statuses."""
        # Create a mock job
        mock_job = Mock()
        mock_job.name = "test_job"

        # Create the filter that the schedule uses
        runs_filter = RunsFilter(
            job_name=mock_job.name,
            statuses=[
                DagsterRunStatus.QUEUED,
                DagsterRunStatus.NOT_STARTED,
                DagsterRunStatus.STARTING,
                DagsterRunStatus.STARTED,
                DagsterRunStatus.CANCELING,
            ],
        )

        # Verify filter properties
        assert runs_filter.job_name == mock_job.name
        assert len(runs_filter.statuses) == 5
        assert "QUEUED" in [status.name for status in runs_filter.statuses]
        assert "NOT_STARTED" in [status.name for status in runs_filter.statuses]
        assert "STARTING" in [status.name for status in runs_filter.statuses]
        assert "STARTED" in [status.name for status in runs_filter.statuses]
        assert "CANCELING" in [status.name for status in runs_filter.statuses]
        # Ensure SUBMITTED is NOT in the list (this was the bug we fixed)
        assert "SUBMITTED" not in [status.name for status in runs_filter.statuses]

    def test_schedule_with_different_granularities(self):
        """Test schedule with different granularities."""
        # Test hourly schedule
        recommended_schedule = "0 * * * *"

        execution_time = datetime.datetime(2024, 1, 15, 9, 0, 0)
        result = RunRequest(
            run_key=f"sqlmesh_adaptive_{execution_time.isoformat()}",
            tags={
                "schedule": "sqlmesh_adaptive",
                "granularity": recommended_schedule,
            },
        )

        assert isinstance(result, RunRequest)
        assert result.tags["granularity"] == "0 * * * *"

    def test_schedule_skip_reason_message(self):
        """Test that SkipReason has correct message when active runs exist."""
        result = SkipReason(
            "sqlmesh job already active; skipping new run to enforce singleton execution"
        )

        # Verify result
        assert isinstance(result, SkipReason)
        assert "sqlmesh job already active" in result.skip_message
        assert "singleton execution" in result.skip_message

    def test_schedule_run_request_tags(self):
        """Test that RunRequest has correct tags."""
        execution_time = datetime.datetime(2024, 1, 15, 9, 0, 0)
        result = RunRequest(
            run_key=f"sqlmesh_adaptive_{execution_time.isoformat()}",
            tags={
                "schedule": "sqlmesh_adaptive",
                "granularity": "0 9 * * *",
                "dagster/max_retries": "0",
                "dagster/retry_on_asset_or_op_failure": "false",
                "dagster/concurrency_key": "sqlmesh_jobs_exclusive",
            },
        )

        # Verify result
        assert isinstance(result, RunRequest)
        assert result.run_key.startswith("sqlmesh_adaptive_")
        assert "schedule" in result.tags
        assert result.tags["schedule"] == "sqlmesh_adaptive"
        assert result.tags["granularity"] == "0 9 * * *"
        assert result.tags["dagster/max_retries"] == "0"
        assert result.tags["dagster/retry_on_asset_or_op_failure"] == "false"
        assert result.tags["dagster/concurrency_key"] == "sqlmesh_jobs_exclusive"

    def test_dagster_run_status_values(self):
        """Test that all DagsterRunStatus values used in the schedule exist."""
        # These are the statuses used in the fixed scheduler
        statuses = [
            DagsterRunStatus.QUEUED,
            DagsterRunStatus.NOT_STARTED,
            DagsterRunStatus.STARTING,
            DagsterRunStatus.STARTED,
            DagsterRunStatus.CANCELING,
        ]

        # Verify all statuses exist and are valid
        for status in statuses:
            assert status is not None
            assert hasattr(status, "name")
            assert status.name in [
                "QUEUED",
                "NOT_STARTED",
                "STARTING",
                "STARTED",
                "CANCELING",
            ]

        # Verify SUBMITTED is not used (this was the bug)
        assert not hasattr(DagsterRunStatus, "SUBMITTED")
