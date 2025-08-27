"""
Tests pour les utilitaires d'exécution des assets SQLMesh.
"""

import pytest
from unittest.mock import Mock
from dagster import AssetExecutionContext, AssetKey, MaterializeResult
from dg_sqlmesh.sqlmesh_asset_execution_utils import (
    execute_sqlmesh_materialization,
    process_sqlmesh_results,
    check_model_status,
    create_materialize_result,
    handle_audit_failures,
    handle_successful_execution,
)


class TestExecuteSQLMeshMaterialization:
    """Tests pour execute_sqlmesh_materialization."""

    def test_execute_sqlmesh_materialization_success(self):
        """Test de l'exécution réussie de la matérialisation SQLMesh."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        context.log.debug = Mock()

        sqlmesh = Mock()
        sqlmesh.get_models.return_value = [Mock(name="test_model")]
        sqlmesh.translator = Mock()
        sqlmesh.materialize_assets_threaded.return_value = Mock()
        sqlmesh._process_failed_models_events.return_value = []
        sqlmesh._console.get_skipped_models_events.return_value = []
        sqlmesh._console.get_evaluation_events.return_value = []

        sqlmesh_results = Mock()
        sqlmesh_results.store_results = Mock()

        run_id = "test_run_id"
        selected_asset_keys = [AssetKey(["test", "model"])]

        # Mock pour get_models_to_materialize
        with pytest.MonkeyPatch().context() as m:
            m.setattr(
                "dg_sqlmesh.sqlmesh_asset_execution_utils.get_models_to_materialize",
                lambda *args: [Mock(name="test_model")],
            )

            # Act
            result = execute_sqlmesh_materialization(
                context, sqlmesh, sqlmesh_results, run_id, selected_asset_keys
            )

            # Assert
            assert result is not None
            assert "failed_check_results" in result
            assert "skipped_models_events" in result
            assert "evaluation_events" in result
            assert "plan" in result
            sqlmesh_results.store_results.assert_called_once_with(run_id, result)

    def test_execute_sqlmesh_materialization_no_models_found(self):
        """Test quand aucun modèle n'est trouvé."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        context.log.debug = Mock()

        sqlmesh = Mock()
        sqlmesh_results = Mock()
        run_id = "test_run_id"
        selected_asset_keys = [AssetKey(["test", "model"])]

        # Mock pour get_models_to_materialize qui retourne une liste vide
        with pytest.MonkeyPatch().context() as m:
            m.setattr(
                "dg_sqlmesh.sqlmesh_asset_execution_utils.get_models_to_materialize",
                lambda *args: [],
            )

            # Act & Assert
            with pytest.raises(Exception, match="No models found for selected assets"):
                execute_sqlmesh_materialization(
                    context, sqlmesh, sqlmesh_results, run_id, selected_asset_keys
                )


class TestProcessSQLMeshResults:
    """Tests pour process_sqlmesh_results."""

    def test_process_sqlmesh_results_success(self):
        """Test du traitement réussi des résultats SQLMesh."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        context.log.debug = Mock()

        sqlmesh_results = Mock()
        sqlmesh_results.get_results.return_value = {
            "failed_check_results": [Mock()],
            "skipped_models_events": [Mock()],
            "evaluation_events": [Mock()],
        }

        run_id = "test_run_id"

        # Act
        failed_check_results, skipped_models_events, evaluation_events = (
            process_sqlmesh_results(context, sqlmesh_results, run_id)
        )

        # Assert
        assert len(failed_check_results) == 1
        assert len(skipped_models_events) == 1
        assert len(evaluation_events) == 1
        sqlmesh_results.get_results.assert_called_once_with(run_id)


class TestCheckModelStatus:
    """Tests pour check_model_status."""

    def test_check_model_status_skipped(self):
        """Test quand le modèle est ignoré."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.debug = Mock()
        context.log.error = Mock()

        current_model_name = "schema.test_model"  # Format correct pour le parsing
        current_asset_spec = Mock()
        current_asset_spec.key = AssetKey(["test", "model"])

        failed_check_results = []
        skipped_models_events = [
            {"snapshot_names": {'"catalog"."schema"."test_model"'}}
        ]

        # Act
        model_was_skipped, model_has_audit_failures = check_model_status(
            context,
            current_model_name,
            current_asset_spec,
            failed_check_results,
            skipped_models_events,
        )

        # Assert
        assert model_was_skipped is True
        assert model_has_audit_failures is False
        context.log.error.assert_called_once()

    def test_check_model_status_audit_failures(self):
        """Test quand le modèle a des échecs d'audit."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.debug = Mock()
        context.log.error = Mock()

        current_model_name = "test_model"
        current_asset_spec = Mock()
        current_asset_spec.key = AssetKey(["test", "model"])

        failed_check_result = Mock()
        failed_check_result.asset_key = AssetKey(["test", "model"])
        failed_check_result.metadata = {"audit_message": "Test audit failed"}

        failed_check_results = [failed_check_result]
        skipped_models_events = []

        # Act
        model_was_skipped, model_has_audit_failures = check_model_status(
            context,
            current_model_name,
            current_asset_spec,
            failed_check_results,
            skipped_models_events,
        )

        # Assert
        assert model_was_skipped is False
        assert model_has_audit_failures is True
        context.log.error.assert_called_once()

    def test_check_model_status_success(self):
        """Test quand le modèle s'exécute avec succès."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.debug = Mock()
        context.log.error = Mock()

        current_model_name = "test_model"
        current_asset_spec = Mock()
        current_asset_spec.key = AssetKey(["test", "model"])

        failed_check_results = []
        skipped_models_events = []

        # Act
        model_was_skipped, model_has_audit_failures = check_model_status(
            context,
            current_model_name,
            current_asset_spec,
            failed_check_results,
            skipped_models_events,
        )

        # Assert
        assert model_was_skipped is False
        assert model_has_audit_failures is False
        context.log.error.assert_not_called()


class TestHandleAuditFailures:
    """Tests pour handle_audit_failures."""

    def test_handle_audit_failures_with_checks(self):
        """Test de la gestion des échecs d'audit avec des checks."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        context.log.debug = Mock()
        context.log.warning = Mock()

        current_model_name = "test_model"
        current_asset_spec = Mock()
        current_asset_spec.key = AssetKey(["test", "model"])

        check = Mock()
        check.name = "test_check"
        check.metadata = {"audit_blocking": True}  # Mock metadata as dict
        current_model_checks = [check]

        failed_check_result = Mock()
        failed_check_result.asset_key = AssetKey(["test", "model"])
        failed_check_result.metadata = {"audit_message": "Test audit failed"}
        failed_check_results = [failed_check_result]

        # Act
        result = handle_audit_failures(
            context,
            current_model_name,
            current_asset_spec,
            current_model_checks,
            failed_check_results,
        )

        # Assert
        assert isinstance(result, MaterializeResult)
        assert result.asset_key == current_asset_spec.key
        assert result.metadata["status"] == "materialization_success_audit_failed"
        assert len(result.check_results) == 1
        assert result.check_results[0].check_name == "test_check"
        assert result.check_results[0].passed is False

    def test_handle_audit_failures_without_checks(self):
        """Test de la gestion des échecs d'audit sans checks."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        context.log.debug = Mock()
        context.log.warning = Mock()

        current_model_name = "test_model"
        current_asset_spec = Mock()
        current_asset_spec.key = AssetKey(["test", "model"])

        current_model_checks = []
        failed_check_results = []

        # Act
        result = handle_audit_failures(
            context,
            current_model_name,
            current_asset_spec,
            current_model_checks,
            failed_check_results,
        )

        # Assert
        assert isinstance(result, MaterializeResult)
        assert result.asset_key == current_asset_spec.key
        assert result.metadata["status"] == "materialization_success_audit_failed"
        # Quand il n'y a pas de checks, check_results est une liste vide (Dagster l'ajoute automatiquement)
        assert result.check_results == []


class TestHandleSuccessfulExecution:
    """Tests pour handle_successful_execution."""

    def test_handle_successful_execution_with_checks(self):
        """Test de la gestion du succès avec des checks."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        context.log.debug = Mock()
        context.log.warning = Mock()

        current_model_name = "test_model"
        current_asset_spec = Mock()
        current_asset_spec.key = AssetKey(["test", "model"])

        check = Mock()
        check.name = "test_check"
        check.metadata = {"audit_blocking": True}  # Mock metadata as dict
        current_model_checks = [check]

        evaluation_events = [
            {
                "event_type": "update_snapshot_evaluation",
                "snapshot_name": '"catalog"."schema"."test_model"',
                "num_audits_passed": 1,
                "num_audits_failed": 0,
            }
        ]

        # Act
        result = handle_successful_execution(
            context,
            current_model_name,
            current_asset_spec,
            current_model_checks,
            evaluation_events,
        )

        # Assert
        assert isinstance(result, MaterializeResult)
        assert result.asset_key == current_asset_spec.key
        assert result.metadata["status"] == "success"
        assert len(result.check_results) == 1
        assert result.check_results[0].check_name == "test_check"
        assert result.check_results[0].passed is True

    def test_handle_successful_execution_without_checks(self):
        """Test de la gestion du succès sans checks."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        context.log.debug = Mock()
        context.log.warning = Mock()

        current_model_name = "test_model"
        current_asset_spec = Mock()
        current_asset_spec.key = AssetKey(["test", "model"])

        current_model_checks = []
        evaluation_events = []

        # Act
        result = handle_successful_execution(
            context,
            current_model_name,
            current_asset_spec,
            current_model_checks,
            evaluation_events,
        )

        # Assert
        assert isinstance(result, MaterializeResult)
        assert result.asset_key == current_asset_spec.key
        assert result.metadata["status"] == "success"
        # Quand il n'y a pas de checks, check_results est une liste vide (Dagster l'ajoute automatiquement)
        assert result.check_results == []


class TestCreateMaterializeResult:
    """Tests pour create_materialize_result."""

    def test_create_materialize_result_skipped(self):
        """Test quand le modèle est ignoré."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.error = Mock()
        context.log.debug = Mock()

        current_model_name = "test_model"
        current_asset_spec = Mock()
        current_asset_spec.key = AssetKey(["test", "model"])
        current_model_checks = []

        model_was_skipped = True
        model_has_audit_failures = False
        failed_check_results = []
        evaluation_events = []

        # Act & Assert
        with pytest.raises(
            Exception, match="Model test_model was skipped due to upstream failures"
        ):
            create_materialize_result(
                context,
                current_model_name,
                current_asset_spec,
                current_model_checks,
                model_was_skipped,
                model_has_audit_failures,
                failed_check_results,
                evaluation_events,
            )

    def test_create_materialize_result_audit_failures(self):
        """Test quand le modèle a des échecs d'audit."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        context.log.debug = Mock()
        context.log.warning = Mock()

        current_model_name = "test_model"
        current_asset_spec = Mock()
        current_asset_spec.key = AssetKey(["test", "model"])

        check = Mock()
        check.name = "test_check"
        check.metadata = {"audit_blocking": True}  # Mock metadata as dict
        current_model_checks = [check]

        model_was_skipped = False
        model_has_audit_failures = True

        failed_check_result = Mock()
        failed_check_result.asset_key = AssetKey(["test", "model"])
        failed_check_result.metadata = {"audit_message": "Test audit failed"}
        failed_check_results = [failed_check_result]
        evaluation_events = []

        # Act
        result = create_materialize_result(
            context,
            current_model_name,
            current_asset_spec,
            current_model_checks,
            model_was_skipped,
            model_has_audit_failures,
            failed_check_results,
            evaluation_events,
        )

        # Assert
        assert isinstance(result, MaterializeResult)
        assert result.asset_key == current_asset_spec.key
        assert result.metadata["status"] == "materialization_success_audit_failed"

    def test_create_materialize_result_success(self):
        """Test quand le modèle s'exécute avec succès."""
        # Arrange
        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        context.log.debug = Mock()
        context.log.warning = Mock()

        current_model_name = "test_model"
        current_asset_spec = Mock()
        current_asset_spec.key = AssetKey(["test", "model"])

        check = Mock()
        check.name = "test_check"
        check.metadata = {"audit_blocking": True}  # Mock metadata as dict
        current_model_checks = [check]

        model_was_skipped = False
        model_has_audit_failures = False
        failed_check_results = []
        evaluation_events = [
            {
                "event_type": "update_snapshot_evaluation",
                "snapshot_name": '"catalog"."schema"."test_model"',
                "num_audits_passed": 1,
                "num_audits_failed": 0,
            }
        ]

        # Act
        result = create_materialize_result(
            context,
            current_model_name,
            current_asset_spec,
            current_model_checks,
            model_was_skipped,
            model_has_audit_failures,
            failed_check_results,
            evaluation_events,
        )

        # Assert
        assert isinstance(result, MaterializeResult)
        assert result.asset_key == current_asset_spec.key
        assert result.metadata["status"] == "success"


class TestPhase1HelperFunctions:
    """Unit tests for internal helper functions introduced in Phase 1."""

    def test__select_models_to_materialize_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from dg_sqlmesh.sqlmesh_asset_execution_utils import (
            _select_models_to_materialize,
        )

        sqlmesh = Mock()
        selected_asset_keys = [AssetKey(["db", "schema", "model"])]

        # Patch get_models_to_materialize to return a non-empty list
        monkeypatch.setattr(
            "dg_sqlmesh.sqlmesh_asset_execution_utils.get_models_to_materialize",
            lambda *_args, **_kwargs: [Mock(name="model1")],
        )

        models = _select_models_to_materialize(selected_asset_keys, sqlmesh)  # type: ignore[arg-type]
        assert isinstance(models, list) and len(models) == 1

    def test__select_models_to_materialize_raises_on_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from dg_sqlmesh.sqlmesh_asset_execution_utils import (
            _select_models_to_materialize,
        )

        sqlmesh = Mock()
        selected_asset_keys = [AssetKey(["db", "schema", "model"])]

        # Patch get_models_to_materialize to return empty
        monkeypatch.setattr(
            "dg_sqlmesh.sqlmesh_asset_execution_utils.get_models_to_materialize",
            lambda *_args, **_kwargs: [],
        )

        with pytest.raises(Exception, match="No models found for selected assets"):
            _select_models_to_materialize(selected_asset_keys, sqlmesh)  # type: ignore[arg-type]

    def test__materialize_and_get_plan(self) -> None:
        from dg_sqlmesh.sqlmesh_asset_execution_utils import _materialize_and_get_plan

        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        context.log.debug = Mock()
        sqlmesh = Mock()
        plan_obj = Mock()
        sqlmesh.materialize_assets_threaded.return_value = plan_obj
        models = [Mock(name="m1"), Mock(name="m2")]

        plan = _materialize_and_get_plan(sqlmesh, models, context)
        assert plan is plan_obj
        sqlmesh.materialize_assets_threaded.assert_called_once()

    def test__init_execution_event_buffers(self) -> None:
        from dg_sqlmesh.sqlmesh_asset_execution_utils import (
            _init_execution_event_buffers,
        )

        context = Mock(spec=AssetExecutionContext)
        context.log.debug = Mock()
        failed, skipped, evals, nb_warn = _init_execution_event_buffers(context)
        assert failed == [] and skipped == [] and evals == [] and nb_warn == []

    def test__get_notifier_failures_success(self) -> None:
        from dg_sqlmesh.sqlmesh_asset_execution_utils import _get_notifier_failures

        # Service-based path
        with pytest.MonkeyPatch().context() as m:
            m.setattr(
                "dg_sqlmesh.sqlmesh_asset_execution_utils._get_notifier_failures_noarg",
                lambda: [{"model": "s.m", "audit": "a1"}],
            )
            failures = _get_notifier_failures(Mock())
            assert failures == [{"model": "s.m", "audit": "a1"}]

    def test__get_notifier_failures_exception(self) -> None:
        from dg_sqlmesh.sqlmesh_asset_execution_utils import _get_notifier_failures

        with pytest.MonkeyPatch().context() as m:

            def raise_err():
                raise Exception("boom")

            m.setattr("dg_sqlmesh.notifier_service.get_audit_failures", raise_err)
            failures = _get_notifier_failures(Mock())
            assert failures == []

    def test__summarize_notifier_failures_logs(self) -> None:
        from dg_sqlmesh.execution_notifier import _summarize_notifier_failures

        context = Mock(spec=AssetExecutionContext)
        context.log.info = Mock()
        failures = [
            {"model": "s.m1", "audit": "a1", "blocking": True, "count": 1},
            {"model": "s.m2", "audit": "a2", "blocking": False, "count": 0},
        ]
        _summarize_notifier_failures(context, failures)
        assert context.log.info.called

        # No log when empty
        context2 = Mock(spec=AssetExecutionContext)
        context2.log.info = Mock()
        _summarize_notifier_failures(context2, [])
        context2.log.info.assert_not_called()

    def test__compute_blocking_and_downstream(self) -> None:
        from dg_sqlmesh.execution_downstream import _compute_blocking_and_downstream

        sqlmesh = Mock()
        # Mock model resolution and key mapping
        model_obj = Mock()
        sqlmesh.context.get_model.return_value = model_obj
        failing_key = AssetKey(["db", "schema", "m1"])
        downstream_key = AssetKey(["db", "schema", "m2"])
        sqlmesh.translator.get_asset_key.return_value = failing_key
        # Downstream calculation returns both failing and downstream
        sqlmesh._get_affected_downstream_assets.return_value = {
            failing_key,
            downstream_key,
        }

        notifier_failures = [{"model": "schema.m1", "audit": "a1", "blocking": True}]
        blocking_keys, affected = _compute_blocking_and_downstream(
            sqlmesh, notifier_failures
        )
        assert blocking_keys == [failing_key]
        assert affected == {downstream_key}

    def test__compute_blocking_and_downstream_exception(self) -> None:
        from dg_sqlmesh.execution_downstream import _compute_blocking_and_downstream

        sqlmesh = Mock()
        # translator path returns key
        failing_key = AssetKey(["db", "schema", "m1"])
        sqlmesh.context.get_model.return_value = Mock()
        sqlmesh.translator.get_asset_key.return_value = failing_key
        # Simulate exception in downstream computation
        sqlmesh._get_affected_downstream_assets.side_effect = Exception("x")

        notifier_failures = [{"model": "schema.m1", "audit": "a1", "blocking": True}]
        blocking_keys, affected = _compute_blocking_and_downstream(
            sqlmesh, notifier_failures
        )
        assert blocking_keys == [failing_key]
        assert affected == set()

    def test__build_shared_results_shape(self) -> None:
        from dg_sqlmesh.sqlmesh_asset_execution_utils import _build_shared_results

        plan = Mock()
        results = _build_shared_results(
            plan,
            [],
            [],
            [],
            [],
            [{"model": "s.m", "audit": "a1", "blocking": True}],
            set(),
        )
        # keys present
        assert set(results.keys()) == {
            "failed_check_results",
            "skipped_models_events",
            "evaluation_events",
            "non_blocking_audit_warnings",
            "notifier_audit_failures",
            "affected_downstream_asset_keys",
            "plan",
        }
        assert isinstance(results["affected_downstream_asset_keys"], list)
