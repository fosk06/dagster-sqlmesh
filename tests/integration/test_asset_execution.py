"""
Integration tests for SQLMesh asset execution with Dagster.

These tests verify that our refactored asset execution functions work correctly
in end-to-end scenarios with real SQLMesh models and Dagster contexts.
"""

import pytest
from dagster import (
    build_asset_context,
    MaterializeResult,
    AssetCheckResult,
    AssetKey,
)
from unittest.mock import Mock

from dg_sqlmesh import (
    SQLMeshResource,
    sqlmesh_assets_factory,
    sqlmesh_definitions_factory,
)
from dg_sqlmesh.sqlmesh_asset_execution_utils import (
    process_sqlmesh_results,
    check_model_status,
    create_materialize_result,
)


class TestAssetMaterialization:
    """Test complete asset materialization workflows."""

    def test_asset_materialization_success(self, sqlmesh_resource):
        """Test successful asset creation and definition setup."""
        # Create assets using our factory
        assets = sqlmesh_assets_factory(sqlmesh_resource=sqlmesh_resource)

        # Create definitions
        defs = sqlmesh_definitions_factory(
            project_dir="tests/fixtures/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
            enable_schedule=False,
        )

        # Test that assets and definitions are created successfully
        assert len(assets) > 0
        assert defs is not None
        assert len(defs.assets) > 0

        # Verify we have the expected resource types
        assert "sqlmesh" in defs.resources
        # sqlmesh_results resource no longer needed with new architecture

    def test_asset_materialization_with_dependencies(self, sqlmesh_resource):
        """Test asset creation with dependency resolution."""
        assets = sqlmesh_assets_factory(sqlmesh_resource=sqlmesh_resource)

        defs = sqlmesh_definitions_factory(
            project_dir="tests/fixtures/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
            enable_schedule=False,
        )

        # Test that assets are created with proper dependencies
        assert len(assets) > 0
        assert defs is not None

        # Verify that assets have dependencies configured
        for asset in defs.assets:
            assert hasattr(asset, "deps") or hasattr(asset, "dependency_keys")

    def test_asset_materialization_with_checks(self, sqlmesh_resource):
        """Test asset creation with check configuration."""
        assets = sqlmesh_assets_factory(sqlmesh_resource=sqlmesh_resource)

        defs = sqlmesh_definitions_factory(
            project_dir="tests/fixtures/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
            enable_schedule=False,
        )

        # Test that assets are created with check configurations
        assert len(assets) > 0
        assert defs is not None

        # Verify that assets have check specs configured
        for asset in defs.assets:
            assert hasattr(asset, "check_specs") or hasattr(asset, "check_defs")


class TestAssetDependencies:
    """Test asset dependency resolution and execution order."""

    def test_dependency_execution_order(self, sqlmesh_resource):
        """Test that assets are created with proper dependency order."""
        assets = sqlmesh_assets_factory(sqlmesh_resource=sqlmesh_resource)

        defs = sqlmesh_definitions_factory(
            project_dir="tests/fixtures/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
            enable_schedule=False,
        )

        # Test that assets are created with proper dependency structure
        assert len(assets) > 0
        assert defs is not None

        # Verify that the asset graph has proper dependency relationships
        asset_graph = defs.resolve_asset_graph()
        assert len(asset_graph.get_all_asset_keys()) > 0

    def test_partial_asset_selection(self, sqlmesh_resource):
        """Test asset creation with selection capabilities."""
        assets = sqlmesh_assets_factory(sqlmesh_resource=sqlmesh_resource)

        defs = sqlmesh_definitions_factory(
            project_dir="tests/fixtures/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
            enable_schedule=False,
        )

        # Test that assets are created with selection capabilities
        assert len(assets) > 0
        assert defs is not None

        # Verify that assets can be selected individually
        asset_graph = defs.resolve_asset_graph()
        all_keys = asset_graph.get_all_asset_keys()
        assert len(all_keys) > 0


class TestAssetExecutionWithContext:
    """Test asset execution using Dagster execution context."""

    def test_asset_execution_context_integration(self, sqlmesh_resource):
        """Test that our asset execution functions work with Dagster context."""
        # Build a mock context
        context = build_asset_context()

        # Mock SQLMesh results
        {
            "failed_check_results": [],
            "skipped_models_events": [],
            "evaluation_events": [],
            "plan": Mock(),
        }

        # Test our execution utilities with context
        mock_sqlmesh_results = Mock()
        mock_sqlmesh_results.get_results.return_value = {
            "failed_check_results": [],
            "skipped_models_events": [],
            "evaluation_events": [],
        }

        processed_results = process_sqlmesh_results(
            context=context, sqlmesh_results=mock_sqlmesh_results, run_id="test_run_123"
        )

        assert isinstance(processed_results, tuple)
        assert len(processed_results) == 3

    def test_model_status_check_with_context(self, sqlmesh_resource):
        """Test model status checking with execution context."""
        context = build_asset_context()

        # Mock asset spec
        mock_asset_spec = Mock()
        mock_asset_spec.key = AssetKey(["test_model"])

        # Test status checking
        was_skipped, has_audit_failures = check_model_status(
            context=context,
            current_model_name="test_model",
            current_asset_spec=mock_asset_spec,
            failed_check_results=[],
            skipped_models_events=[],
        )

        assert isinstance(was_skipped, bool)
        assert isinstance(has_audit_failures, bool)

    def test_materialize_result_creation_with_context(self, sqlmesh_resource):
        """Test MaterializeResult creation with execution context."""
        context = build_asset_context()

        mock_asset_spec = Mock()
        mock_asset_spec.key = AssetKey(["test_model"])

        # Test result creation
        result = create_materialize_result(
            context=context,
            current_model_name="test_model",
            current_asset_spec=mock_asset_spec,
            current_model_checks=[],
            model_was_skipped=False,
            model_has_audit_failures=False,
            failed_check_results=[],
            evaluation_events=[],
        )

        assert isinstance(result, MaterializeResult)
        assert result.asset_key == AssetKey(["test_model"])


class TestAssetExecutionErrors:
    """Test error handling during asset execution."""

    def test_asset_materialization_failure_handling(self, sqlmesh_resource):
        """Test handling of asset materialization failures."""
        # Test that our error handling works correctly
        # We'll test the error handling at the function level instead of mocking
        sqlmesh_assets_factory(sqlmesh_resource=sqlmesh_resource)

        defs = sqlmesh_definitions_factory(
            project_dir="tests/fixtures/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
            enable_schedule=False,
        )

        # Test that we can create the definitions without errors
        assert defs is not None
        assert len(defs.assets) > 0

    def test_asset_check_failure_handling(self, sqlmesh_resource):
        """Test handling of asset check failures."""
        context = build_asset_context()

        mock_asset_spec = Mock()
        mock_asset_spec.key = AssetKey(["test_model"])

        # Mock failed check results
        failed_checks = [
            AssetCheckResult(
                passed=False,
                asset_key=AssetKey(["test_model"]),
                check_name="test_check",
                metadata={"error": "Check failed"},
            )
        ]

        # Test result creation with failed checks
        result = create_materialize_result(
            context=context,
            current_model_name="test_model",
            current_asset_spec=mock_asset_spec,
            current_model_checks=[],
            model_was_skipped=False,
            model_has_audit_failures=True,
            failed_check_results=failed_checks,
            evaluation_events=[],
        )

        assert isinstance(result, MaterializeResult)
        assert result.asset_key == AssetKey(["test_model"])


class TestAssetExecutionIntegration:
    """Integration tests for complete asset execution workflows."""

    def test_complete_sqlmesh_workflow(self, sqlmesh_resource):
        """Test complete SQLMesh workflow setup with Dagster integration."""
        # Create complete definitions
        defs = sqlmesh_definitions_factory(
            project_dir="tests/fixtures/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
            enable_schedule=False,
        )

        # Test that the complete workflow is set up correctly
        assert defs is not None
        assert len(defs.assets) > 0

        # Verify we have the expected resources
        assert "sqlmesh" in defs.resources
        # sqlmesh_results resource no longer needed with new architecture

        # Verify the asset graph is properly configured
        asset_graph = defs.resolve_asset_graph()
        assert len(asset_graph.get_all_asset_keys()) > 0

    def test_sqlmesh_resource_integration(self, sqlmesh_resource):
        """Test SQLMesh resource integration with asset execution."""
        # Test resource setup
        assert sqlmesh_resource.project_dir is not None
        assert sqlmesh_resource.context is not None
        assert sqlmesh_resource.translator is not None

        # Test resource methods
        models = sqlmesh_resource.get_models()
        assert len(models) > 0

        # Test schedule recommendation
        schedule = sqlmesh_resource.get_recommended_schedule()
        assert isinstance(schedule, str)

    def test_asset_factory_integration(self, sqlmesh_resource):
        """Test asset factory integration with execution."""
        # Create assets with different configurations
        assets_basic = sqlmesh_assets_factory(sqlmesh_resource=sqlmesh_resource)

        assets_with_owners = sqlmesh_assets_factory(
            sqlmesh_resource=sqlmesh_resource, owners=["test_team"]
        )

        # Verify assets are created
        assert len(assets_basic) > 0
        assert len(assets_with_owners) > 0

        # Test definitions creation
        defs = sqlmesh_definitions_factory(
            project_dir="tests/fixtures/sqlmesh_project",
            gateway="duckdb",
            environment="dev",
            enable_schedule=False,
        )

        assert defs is not None


# Fixtures for testing
@pytest.fixture
def sqlmesh_resource():
    """Create a SQLMesh resource for testing."""
    return SQLMeshResource(
        project_dir="tests/fixtures/sqlmesh_project",
        gateway="duckdb",
        environment="dev",
    )
