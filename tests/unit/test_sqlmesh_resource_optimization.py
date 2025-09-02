"""
Tests for SQLMesh resource optimization functionality.
"""

import pytest
from unittest.mock import Mock, patch

from dg_sqlmesh.resource import SQLMeshResource
from sqlmesh.utils.errors import NoChangesPlanError


class TestSQLMeshResourceOptimization:
    """Test suite for SQLMesh resource optimization methods."""

    @pytest.fixture
    def mock_sqlmesh_resource(self):
        """Create mock SQLMesh resource."""
        resource = Mock(spec=SQLMeshResource)
        resource._logger = Mock()
        return resource

    def test_has_models_to_execute_with_changes(self, mock_sqlmesh_resource):
        """Test has_models_to_execute returns True when there are changes."""
        # Mock context.plan to return successfully (no exception = changes exist)
        mock_context = Mock()
        mock_context.plan.return_value = Mock()  # Successful plan creation
        mock_sqlmesh_resource.context = mock_context

        # Call the method
        result = SQLMeshResource.has_models_to_execute(mock_sqlmesh_resource)

        # Verify result
        assert result is True
        mock_context.plan.assert_called_once_with(
            select_models=None,
            auto_apply=False,
            no_prompts=True,
        )

    def test_has_models_to_execute_with_specific_models(self, mock_sqlmesh_resource):
        """Test has_models_to_execute with specific model names."""
        # Mock context.plan to return successfully
        mock_context = Mock()
        mock_context.plan.return_value = Mock()
        mock_sqlmesh_resource.context = mock_context

        # Call with specific models
        model_names = ["model1", "model2"]
        result = SQLMeshResource.has_models_to_execute(mock_sqlmesh_resource, model_names)

        # Verify result
        assert result is True
        mock_context.plan.assert_called_once_with(
            select_models=model_names,
            auto_apply=False,
            no_prompts=True,
        )

    def test_has_models_to_execute_no_changes(self, mock_sqlmesh_resource):
        """Test has_models_to_execute returns False when no changes needed."""
        # Mock context.plan to raise NoChangesPlanError
        mock_context = Mock()
        mock_context.plan.side_effect = NoChangesPlanError("No changes needed")
        mock_sqlmesh_resource.context = mock_context

        # Call the method
        result = SQLMeshResource.has_models_to_execute(mock_sqlmesh_resource)

        # Verify result
        assert result is False
        mock_context.plan.assert_called_once_with(
            select_models=None,
            auto_apply=False,
            no_prompts=True,
        )

    def test_has_models_to_execute_other_error(self, mock_sqlmesh_resource):
        """Test has_models_to_execute returns True for other errors (conservative approach)."""
        # Mock context.plan to raise a different error
        mock_context = Mock()
        mock_context.plan.side_effect = Exception("Some other error")
        mock_sqlmesh_resource.context = mock_context

        # Call the method
        result = SQLMeshResource.has_models_to_execute(mock_sqlmesh_resource)

        # Verify result (should be True for conservative approach)
        assert result is True
        mock_context.plan.assert_called_once_with(
            select_models=None,
            auto_apply=False,
            no_prompts=True,
        )
        
        # Verify warning was logged
        mock_sqlmesh_resource._logger.warning.assert_called_once()
        warning_call = mock_sqlmesh_resource._logger.warning.call_args[0][0]
        assert "Error checking for model changes" in warning_call
        assert "Assuming execution needed" in warning_call

    def test_has_models_to_execute_plan_error(self, mock_sqlmesh_resource):
        """Test has_models_to_execute returns True for PlanError (conservative approach)."""
        from sqlmesh.utils.errors import PlanError
        
        # Mock context.plan to raise PlanError
        mock_context = Mock()
        mock_context.plan.side_effect = PlanError("Plan error")
        mock_sqlmesh_resource.context = mock_context

        # Call the method
        result = SQLMeshResource.has_models_to_execute(mock_sqlmesh_resource)

        # Verify result (should be True for conservative approach)
        assert result is True
        mock_context.plan.assert_called_once_with(
            select_models=None,
            auto_apply=False,
            no_prompts=True,
        )
        
        # Verify warning was logged
        mock_sqlmesh_resource._logger.warning.assert_called_once()

    def test_has_models_to_execute_multiple_scenarios(self, mock_sqlmesh_resource):
        """Test has_models_to_execute with multiple scenarios."""
        mock_context = Mock()
        mock_sqlmesh_resource.context = mock_context
        mock_sqlmesh_resource._logger = Mock()

        # Test case 1: No changes
        mock_context.plan.side_effect = NoChangesPlanError("No changes needed")
        result = SQLMeshResource.has_models_to_execute(mock_sqlmesh_resource)
        assert result is False

        # Test case 2: Changes exist
        mock_context.plan.side_effect = None
        mock_context.plan.return_value = Mock()
        result = SQLMeshResource.has_models_to_execute(mock_sqlmesh_resource)
        assert result is True

        # Test case 3: With specific models
        model_names = ["users", "orders"]
        result = SQLMeshResource.has_models_to_execute(mock_sqlmesh_resource, model_names)
        assert result is True
        mock_context.plan.assert_called_with(
            select_models=model_names,
            auto_apply=False,
            no_prompts=True,
        )
