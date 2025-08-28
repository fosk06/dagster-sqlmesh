"""
Tests pour la fonctionnalité sqlmesh_execution_status.

Cette fonctionnalité ajoute automatiquement un AssetCheck "sqlmesh_execution_status"
pour tous les modèles SQLMesh, indiquant s'ils ont été exécutés ou skippés.
"""

from unittest.mock import Mock
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetCheckSeverity,
)
from dg_sqlmesh.sqlmesh_asset_check_utils import create_asset_checks_from_model
from dg_sqlmesh.sqlmesh_asset_execution_utils import handle_successful_execution


class TestSQLMeshExecutionStatusCheck:
    """Tests pour le check sqlmesh_execution_status."""

    def test_sqlmesh_execution_status_check_creation(self):
        """Test que le check sqlmesh_execution_status est créé pour tous les modèles."""
        # Arrange
        mock_model = Mock()
        mock_model.name = "test_schema.test_model"
        mock_model.audits = [("test_audit", {})]

        # Mock audits_with_args to return a list of tuples
        mock_audit = Mock()
        mock_audit.name = "test_audit"
        mock_model.audits_with_args = [(mock_audit, {})]

        asset_key = AssetKey(["test_db", "test_schema", "test_model"])

        # Act
        check_specs = create_asset_checks_from_model(mock_model, asset_key)

        # Assert
        # Vérifier qu'on a le check sqlmesh_execution_status en plus des audits
        assert len(check_specs) == 2  # 1 audit + 1 sqlmesh_execution_status

        # Trouver le check sqlmesh_execution_status
        execution_status_check = next(
            check for check in check_specs if check.name == "sqlmesh_execution_status"
        )

        assert execution_status_check is not None
        assert execution_status_check.asset_key == asset_key
        assert execution_status_check.blocking is False
        assert execution_status_check.metadata["check_type"] == "execution_status"
        assert (
            execution_status_check.metadata["sqlmesh_model"] == "test_schema.test_model"
        )

    def test_sqlmesh_execution_status_check_for_model_without_audits(self):
        """Test que le check sqlmesh_execution_status est créé même pour les modèles sans audits."""
        # Arrange
        mock_model = Mock()
        mock_model.name = "test_schema.test_model_no_audits"
        mock_model.audits = []
        mock_model.audits_with_args = []

        asset_key = AssetKey(["test_db", "test_schema", "test_model_no_audits"])

        # Act
        check_specs = create_asset_checks_from_model(mock_model, asset_key)

        # Assert
        # Vérifier qu'on a seulement le check sqlmesh_execution_status
        assert len(check_specs) == 1

        execution_status_check = check_specs[0]
        assert execution_status_check.name == "sqlmesh_execution_status"
        assert execution_status_check.asset_key == asset_key
        assert execution_status_check.blocking is False

    def test_sqlmesh_execution_status_check_metadata(self):
        """Test que les métadonnées du check sqlmesh_execution_status sont correctes."""
        # Arrange
        mock_model = Mock()
        mock_model.name = "complex.schema.model_name"
        mock_model.audits = []
        mock_model.audits_with_args = []

        asset_key = AssetKey(["db", "complex", "schema", "model_name"])

        # Act
        check_specs = create_asset_checks_from_model(mock_model, asset_key)

        # Assert
        execution_status_check = check_specs[0]
        metadata = execution_status_check.metadata

        assert metadata["check_type"] == "execution_status"
        assert metadata["sqlmesh_model"] == "complex.schema.model_name"
        assert len(metadata) == 2  # Seulement ces deux champs


class TestSQLMeshExecutionStatusCheckResults:
    """Tests pour les résultats du check sqlmesh_execution_status."""

    def test_execution_status_check_success_when_model_executed(self):
        """Test que le check retourne SUCCESS quand le modèle est exécuté."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        context.log.debug = Mock()

        current_model_name = "test_schema.test_model"
        current_asset_spec = Mock()
        current_asset_spec.key = AssetKey(["test_db", "test_schema", "test_model"])

        # Create a proper mock with metadata
        mock_check = Mock()
        mock_check.name = "sqlmesh_execution_status"
        mock_check.metadata = {
            "sqlmesh_model": "test_schema.test_model",
            "check_type": "execution_status",
        }
        current_model_checks = [mock_check]

        # Modèle exécuté par SQLMesh
        sqlmesh_executed_models = ["test_schema.test_model"]

        # Act
        result = handle_successful_execution(
            context=context,
            current_model_name=current_model_name,
            current_asset_spec=current_asset_spec,
            current_model_checks=current_model_checks,
            non_blocking_audit_warnings=[],
            notifier_audit_failures=[],
            sqlmesh_executed_models=sqlmesh_executed_models,
        )

        # Assert
        assert result is not None
        assert result.check_results is not None

        # Trouver le check sqlmesh_execution_status
        execution_status_check = next(
            check
            for check in result.check_results
            if check.check_name == "sqlmesh_execution_status"
        )

        assert execution_status_check is not None
        assert execution_status_check.passed is True
        assert (
            execution_status_check.severity == AssetCheckSeverity.WARN
        )  # WARN since INFO doesn't exist

    def test_execution_status_check_warning_when_model_skipped(self):
        """Test que le check retourne WARNING quand le modèle est skippé."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        context.log.debug = Mock()

        current_model_name = "test_schema.test_model"
        current_asset_spec = Mock()
        current_asset_spec.key = AssetKey(["test_db", "test_schema", "test_model"])

        # Create a proper mock with metadata
        mock_check = Mock()
        mock_check.name = "sqlmesh_execution_status"
        mock_check.metadata = {
            "sqlmesh_model": "test_schema.test_model",
            "check_type": "execution_status",
        }
        current_model_checks = [mock_check]

        # Modèle skippé par SQLMesh
        sqlmesh_executed_models = ["other_schema.other_model"]

        # Act
        result = handle_successful_execution(
            context=context,
            current_model_name=current_model_name,
            current_asset_spec=current_asset_spec,
            current_model_checks=current_model_checks,
            non_blocking_audit_warnings=[],
            notifier_audit_failures=[],
            sqlmesh_executed_models=sqlmesh_executed_models,
        )

        # Assert
        assert result is not None
        assert result.check_results is not None

        # Trouver le check sqlmesh_execution_status
        execution_status_check = next(
            check
            for check in result.check_results
            if check.check_name == "sqlmesh_execution_status"
        )

        assert execution_status_check is not None
        assert execution_status_check.passed is False
        assert execution_status_check.severity == AssetCheckSeverity.WARN

    def test_execution_status_check_with_multiple_models(self):
        """Test que le check fonctionne avec plusieurs modèles."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        context.log.debug = Mock()

        current_model_name = "test_schema.model_b"
        current_asset_spec = Mock()
        current_asset_spec.key = AssetKey(["test_db", "test_schema", "model_b"])

        # Create a proper mock with metadata
        mock_check = Mock()
        mock_check.name = "sqlmesh_execution_status"
        mock_check.metadata = {
            "sqlmesh_model": "test_schema.model_b",
            "check_type": "execution_status",
        }
        current_model_checks = [mock_check]

        # Plusieurs modèles exécutés, mais pas celui-ci
        sqlmesh_executed_models = [
            "test_schema.model_a",
            "test_schema.model_c",
            "other_schema.other_model",
        ]

        # Act
        result = handle_successful_execution(
            context=context,
            current_model_name=current_model_name,
            current_asset_spec=current_asset_spec,
            current_model_checks=current_model_checks,
            non_blocking_audit_warnings=[],
            notifier_audit_failures=[],
            sqlmesh_executed_models=sqlmesh_executed_models,
        )

        # Assert
        assert result is not None
        assert result.check_results is not None

        execution_status_check = next(
            check
            for check in result.check_results
            if check.check_name == "sqlmesh_execution_status"
        )

        # Model B n'était pas dans la liste des exécutés, donc WARNING
        assert execution_status_check.passed is False
        assert execution_status_check.severity == AssetCheckSeverity.WARN

    def test_execution_status_check_without_check_spec(self):
        """Test que handle_successful_execution fonctionne même sans check sqlmesh_execution_status."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        context.log.debug = Mock()

        current_model_name = "test_schema.test_model"
        current_asset_spec = Mock()
        current_asset_spec.key = AssetKey(["test_db", "test_schema", "test_model"])

        # Pas de check sqlmesh_execution_status
        current_model_checks = []

        sqlmesh_executed_models = ["test_schema.test_model"]

        # Act
        result = handle_successful_execution(
            context=context,
            current_model_name=current_model_name,
            current_asset_spec=current_asset_spec,
            current_model_checks=current_model_checks,
            non_blocking_audit_warnings=[],
            notifier_audit_failures=[],
            sqlmesh_executed_models=sqlmesh_executed_models,
        )

        # Assert
        assert result is not None
        # Pas de check results car pas de checks
        assert result.check_results is None or len(result.check_results) == 0


class TestSQLMeshExecutionStatusIntegration:
    """Tests d'intégration pour sqlmesh_execution_status."""

    def test_execution_status_check_with_real_model_structure(self):
        """Test avec une structure de modèle plus réaliste."""
        # Arrange
        mock_model = Mock()
        mock_model.name = "jaffle_platform.stg_customers"
        mock_model.audits = [
            ("not_null", {"columns": ["customer_id", "name"]}),
            ("number_of_rows", {"threshold": 100}),
        ]

        # Mock audits_with_args
        mock_not_null_audit = Mock()
        mock_not_null_audit.name = "not_null"
        mock_number_of_rows_audit = Mock()
        mock_number_of_rows_audit.name = "number_of_rows"
        mock_model.audits_with_args = [
            (mock_not_null_audit, {"columns": ["customer_id", "name"]}),
            (mock_number_of_rows_audit, {"threshold": 100}),
        ]

        asset_key = AssetKey(["jaffle_db", "jaffle_platform", "stg_customers"])

        # Act
        check_specs = create_asset_checks_from_model(mock_model, asset_key)

        # Assert
        # 2 audits + 1 sqlmesh_execution_status
        assert len(check_specs) == 3

        # Vérifier que tous les checks ont le bon asset_key
        for check_spec in check_specs:
            assert check_spec.asset_key == asset_key
            assert check_spec.blocking is False

        # Vérifier la présence du check sqlmesh_execution_status
        execution_status_check = next(
            check for check in check_specs if check.name == "sqlmesh_execution_status"
        )

        assert (
            execution_status_check.metadata["sqlmesh_model"]
            == "jaffle_platform.stg_customers"
        )
        assert execution_status_check.metadata["check_type"] == "execution_status"
