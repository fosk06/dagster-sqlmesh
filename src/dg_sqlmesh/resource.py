import anyio
import logging
import datetime
from typing import Any
from pydantic import PrivateAttr
from dagster import (
    AssetKey,
    AssetCheckResult,
    MaterializeResult,
    DataVersion,
    ConfigurableResource,
    InitResourceContext,
    Failure,
)
from sqlmesh import Context
from .translator import SQLMeshTranslator
from .sqlmesh_asset_utils import (
    get_models_to_materialize,
    get_topologically_sorted_asset_keys,
    format_partition_metadata,
    get_model_partitions_from_plan,
    analyze_sqlmesh_crons_using_api,
    get_model_from_asset_key,
)
from .notifier_service import (
    get_or_create_notifier,
    register_notifier_in_context,
)
from .sqlmesh_asset_check_utils import (
    deduplicate_asset_check_results,
    serialize_audit_args,
    create_failed_audit_check_result,
    create_general_error_check_result,
    convert_notifier_failures_to_asset_check_results,
)
from sqlmesh.utils.errors import (
    SQLMeshError,
    PlanError,
    ConflictingPlanError,
    NodeAuditsErrors,
    CircuitBreakerError,
    NoChangesPlanError,
    UncategorizedPlanError,
    AuditError,
    PythonModelEvalError,
    SignalEvalError,
)


class UpstreamAuditFailureError(Failure):
    """
    Custom exception for upstream audit failures that should be handled gracefully.
    This exception is marked as non-retriable (allow_retries=False).
    """

    def __init__(self, description: str | None = None, metadata: dict | None = None):
        super().__init__(
            description=description, metadata=metadata, allow_retries=False
        )


def convert_unix_timestamp_to_readable(timestamp: float | int | None) -> str | None:
    """
    Converts a Unix timestamp to a readable date.

    Args:
        timestamp: Unix timestamp in milliseconds (int or float)

    Returns:
        str: Date in "YYYY-MM-DD HH:MM:SS" format or None if timestamp is None
    """
    if timestamp is None:
        return None

    try:
        # Convert milliseconds to seconds
        timestamp_seconds = timestamp / 1000
        dt = datetime.datetime.fromtimestamp(timestamp_seconds)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        # Fallback if conversion fails
        return str(timestamp)


class SQLMeshResource(ConfigurableResource):
    """
    Dagster resource for interacting with SQLMesh.
    Manages SQLMesh context, caching and orchestrates materialization.
    """

    project_dir: str
    gateway: str = "postgres"
    environment: str = "prod"
    concurrency_limit: int = 1

    # Private attribute for Dagster logger (not subject to Pydantic immutability)
    _logger: Any = PrivateAttr(default=None)

    # Singleton for SQLMesh console (lazy initialized)

    def __init__(self, **kwargs):
        # Extract translator before calling super().__init__
        translator = kwargs.pop("translator", None)
        super().__init__(**kwargs)

        # Store translator for later use
        if translator:
            self._translator_instance = translator

        # No console initialization (migrated to notifier-only)

    def __del__(self):
        pass  # Simplified cleanup

    @property
    def logger(self):
        """Returns the logger for this resource."""
        return logging.getLogger(__name__)

    @classmethod
    def _get_or_create_notifier(_cls):
        """Deprecated internal accessor kept for backward compatibility; delegates to service."""
        return get_or_create_notifier()

    @property
    def context(self) -> Context:
        """
        Returns the SQLMesh context. Cached for performance.
        """
        if not hasattr(self, "_context_cache"):
            self._context_cache = Context(
                paths=self.project_dir,
                gateway=self.gateway,
            )
            # Register our notifier target at Context init via service (idempotent)
            register_notifier_in_context(self._context_cache)
        return self._context_cache

    @property
    def translator(self) -> SQLMeshTranslator:
        """
        Returns a SQLMeshTranslator instance for mapping AssetKeys and models.
        Cached for performance.
        """
        if not hasattr(self, "_translator_cache"):
            # Use translator provided as parameter or create a new one
            self._translator_cache = (
                getattr(self, "_translator_instance", None) or SQLMeshTranslator()
            )
        return self._translator_cache

    def setup_for_execution(self, context: InitResourceContext) -> None:
        # Store Dagster logger in private attribute
        self._logger = context.log

        # No console configuration

    def get_models(self):
        """
        Returns all SQLMesh models. Cached for performance.
        """
        if not hasattr(self, "_models_cache"):
            self._models_cache = list(self.context.models.values())
        return self._models_cache

    def get_recommended_schedule(self):
        """
        Analyzes SQLMesh crons and returns the recommended Dagster schedule.

        Returns:
            str: Recommended Dagster cron expression
        """
        return analyze_sqlmesh_crons_using_api(self.context)

    def _serialize_audit_args(self, audit_args):
        """Deprecated: use sqlmesh_asset_check_utils.serialize_audit_args"""
        return serialize_audit_args(audit_args)

    def _deduplicate_asset_check_results(
        self, asset_check_results: list[AssetCheckResult]
    ) -> list[AssetCheckResult]:
        """
        Deduplicate AssetCheckResult events to prevent conflicts.
        If an asset has both successful and failed audits, prioritize failed ones.
        """
        if not asset_check_results:
            return []

        # Group by asset_key and check_name
        grouped_results = {}
        for result in asset_check_results:
            key = (result.asset_key, result.check_name)
            if key not in grouped_results:
                grouped_results[key] = result
            else:
                # If we have both passed=True and passed=False for same check, prioritize failed
                if not result.passed and grouped_results[key].passed:
                    grouped_results[key] = result
                    if self._logger:
                        self._logger.warning(
                            f"Conflicting audit results for {result.asset_key}.{result.check_name}: prioritizing failed result"
                        )

        return list(grouped_results.values())

    def materialize_assets(self, models: list[Any], context: Any | None = None) -> Any:
        """Materialize specified SQLMesh models with robust error handling."""
        model_names = [model.name for model in models]
        try:
            # Note: Do NOT clear notifier state here as audit failures may have been
            # captured by a previous SQLMesh run and need to be preserved
            plan = self._create_sqlmesh_plan(model_names)
            self._run_sqlmesh_plan(model_names)
            return plan
        except CircuitBreakerError:
            self._log_and_raise(
                "Run interrupted: environment changed during execution."
            )
        except (
            PlanError,
            ConflictingPlanError,
            NoChangesPlanError,
            UncategorizedPlanError,
        ) as e:
            self._log_and_raise(f"Planning error: {e}")
        except (AuditError, NodeAuditsErrors) as e:
            self._log_and_raise(f"Audit error: {e}")
        except (PythonModelEvalError, SignalEvalError) as e:
            self._log_and_raise(f"Model or signal execution error: {e}")
        except SQLMeshError as e:
            self._log_and_raise(f"SQLMesh error: {e}")
        except Exception as e:
            self._log_and_raise(f"Unexpected error: {e}")

    def _create_sqlmesh_plan(self, model_names: list[str]) -> Any:
        return self.context.plan(
            select_models=model_names,
            auto_apply=False,  # never apply the plan, we will juste need it for metadata collection
            no_prompts=True,
        )

    def _run_sqlmesh_plan(self, model_names: list[str]) -> None:
        self.context.run(
            environment=self.environment,
            select_models=model_names,
            execution_time=datetime.datetime.now(),
        )

    def _log_and_raise(self, message: str) -> None:
        self._logger.error(message)
        raise

    def materialize_assets_threaded(
        self, models: list[Any], context: Any | None = None
    ) -> Any:
        """Synchronous wrapper for Dagster that uses anyio."""

        def run_materialization():
            try:
                return self.materialize_assets(models, context)
            except Exception as e:
                self._logger.error(f"Materialization failed: {e}")
                raise

        return anyio.run(anyio.to_thread.run_sync, run_materialization)

    def _extract_model_info(self, error: Any) -> tuple[str, Any | None, Any | None]:
        """Extract (model_name, model, asset_key) from a SQLMesh error object."""
        model_name = "unknown"
        model = None
        asset_key = None

        try:
            # Handle different error types
            if hasattr(error, "node") and error.node:
                # Standard SQLMesh error with node attribute
                model_name = (
                    error.node[0]
                    if isinstance(error.node, (list, tuple))
                    else str(error.node)
                )
            elif hasattr(error, "__str__"):
                # Try to extract from error message (e.g., NodeExecutionFailedError)
                error_str = str(error)
                # Look for pattern like "Execution failed for node ('"db"."schema"."model"', ...)"
                import re

                match = re.search(r'"([^"]+)"\."([^"]+)"\."([^"]+)"', error_str)
                if match:
                    db, schema, model = match.groups()
                    model_name = f"{schema}.{model}"
                else:
                    # Fallback: try to extract any quoted model name
                    match = re.search(r'"([^"]+\.[^"]+)"', error_str)
                    if match:
                        model_name = match.group(1)
            else:
                model_name = str(error)

            # Try to get model and asset key
            if model_name != "unknown":
                model = self.context.get_model(model_name)
                if model:
                    asset_key = self.translator.get_asset_key(model)
        except Exception as e:
            if self._logger:
                self._logger.warning(f"Error converting model name to asset key: {e}")

        return model_name, model, asset_key

    def _create_failed_audit_check_result(
        self, audit_error, model_name, asset_key
    ) -> AssetCheckResult | None:
        """Deprecated: use sqlmesh_asset_check_utils.create_failed_audit_check_result"""
        return create_failed_audit_check_result(
            audit_error=audit_error,
            model_name=model_name,
            asset_key=asset_key,
            logger=self._logger,
        )

    def _create_general_error_check_result(
        self, error, model_name, asset_key, error_type: str, message: str
    ) -> AssetCheckResult:
        """Deprecated: use sqlmesh_asset_check_utils.create_general_error_check_result"""
        return create_general_error_check_result(
            error=error,
            model_name=model_name,
            asset_key=asset_key,
            error_type=error_type,
            message=message,
            logger=self._logger,
        )

    def _process_notifier_audit_failures(self) -> list[AssetCheckResult]:
        """
        Convert notifier-captured audit failures into AssetCheckResult.
        Honors blocking flag to set severity and downstream blocking.
        """
        try:
            from .notifier_service import get_audit_failures

            failures = get_audit_failures()
        except Exception:
            failures = []

        return convert_notifier_failures_to_asset_check_results(
            context=self.context,
            translator=self.translator,
            failures=failures,
            logger=self._logger,
        )

    def _process_single_error(
        self,
        error: Any,
        model_name: str,
        asset_key: Any,
        asset_check_results: list[AssetCheckResult],
    ) -> None:
        # Process audit errors if present
        if hasattr(error, "__cause__") and error.__cause__:
            if isinstance(error.__cause__, NodeAuditsErrors):
                self._process_audit_errors(
                    error.__cause__, model_name, asset_key, asset_check_results
                )
            else:
                general_result = self._create_general_error_check_result(
                    error,
                    model_name,
                    asset_key,
                    "general_execution_error",
                    str(error.__cause__),
                )
                asset_check_results.append(general_result)
        else:
            general_result = self._create_general_error_check_result(
                error, model_name, asset_key, "general_model_failure", str(error)
            )
            asset_check_results.append(general_result)

    def _process_audit_errors(
        self,
        audits_errors: Any,
        model_name: str,
        asset_key: Any,
        asset_check_results: list[AssetCheckResult],
    ) -> None:
        for audit_error in audits_errors.errors:
            audit_result = self._create_failed_audit_check_result(
                audit_error, model_name, asset_key
            )
            if audit_result is not None:
                asset_check_results.append(audit_result)

    def _log_failed_error_processing(self, exception: Exception) -> None:
        if self._logger:
            self._logger.warning(f"Failed to process error: {exception}")

    def _get_failed_blocking_checks(
        self, asset_check_results: list[AssetCheckResult]
    ) -> dict[AssetKey, list[AssetCheckResult]]:
        """
        Extract failed checks that are blocking from AssetCheckResult list.

        Args:
            asset_check_results: List of all AssetCheckResult

        Returns:
            Dict mapping AssetKey to list of failed blocking checks
        """
        failed_blocking = {}
        for check_result in asset_check_results:
            if not check_result.passed:
                # Check if this audit is blocking from metadata
                metadata = check_result.metadata
                audit_blocking = metadata.get(
                    "audit_blocking", True
                )  # Default to blocking
                if audit_blocking:
                    asset_key = check_result.asset_key
                    if asset_key not in failed_blocking:
                        failed_blocking[asset_key] = []
                    failed_blocking[asset_key].append(check_result)
        return failed_blocking

    def _get_affected_downstream_assets(
        self, failed_asset_keys: list[AssetKey]
    ) -> set[AssetKey]:
        """
        Get all downstream assets that should be blocked due to upstream failures.

        Args:
            failed_asset_keys: List of AssetKeys that failed

        Returns:
            Set of AssetKeys that should be blocked
        """
        affected_assets = set()

        for failed_asset_key in failed_asset_keys:
            # Get SQLMesh model from asset key
            failed_model = get_model_from_asset_key(
                self.context, self.translator, failed_asset_key
            )
            if failed_model:
                # Get downstream models using our new utility
                from .sqlmesh_asset_utils import get_downstream_models

                downstream_models = get_downstream_models(
                    context=self.context,
                    model=failed_model,
                    selected_models=None,  # All downstream
                )

                # Convert to AssetKeys
                for downstream_model in downstream_models:
                    downstream_asset_key = self.translator.get_asset_key(
                        downstream_model
                    )
                    affected_assets.add(downstream_asset_key)

        return affected_assets

    def materialize_all_assets(self, context):
        """
        Materializes all selected assets and yields results.
        """

        selected_asset_keys = context.selected_asset_keys
        models_to_materialize = get_models_to_materialize(
            selected_asset_keys,
            self.get_models,
            self.translator,
        )

        # Create and apply plan
        plan = self.materialize_assets_threaded(models_to_materialize, context=context)

        # Extract categorized snapshots directly from plan
        assetkey_to_snapshot = {}
        for snapshot in plan.snapshots.values():
            model = snapshot.model
            asset_key = self.translator.get_asset_key(model)
            assetkey_to_snapshot[asset_key] = snapshot

        # Sort asset keys in topological order
        ordered_asset_keys = get_topologically_sorted_asset_keys(
            self.context, self.translator, selected_asset_keys
        )

        # No audit successes gathered from console (handled via factory by default)
        successful_audit_results: list[AssetCheckResult] = []

        # Legacy console path removed
        failed_models_results: list[AssetCheckResult] = []

        # Process audits captured via notifier (blocking and non-blocking)
        notifier_audit_results = self._process_notifier_audit_failures()

        # No console warning path
        non_blocking_warning_results: list[AssetCheckResult] = []

        # Combine and deduplicate all AssetCheckResult events
        all_asset_check_results = (
            successful_audit_results
            + failed_models_results
            + non_blocking_warning_results
            + notifier_audit_results
        )
        deduplicated_results = deduplicate_asset_check_results(
            all_asset_check_results, logger=self._logger
        )

        # Get failed blocking checks and affected downstream assets
        failed_blocking_checks = self._get_failed_blocking_checks(deduplicated_results)
        failed_asset_keys = list(failed_blocking_checks.keys())
        affected_downstream = self._get_affected_downstream_assets(failed_asset_keys)

        # Group AssetCheckResult by asset_key for MaterializeResult
        asset_check_results_by_key = {}
        for asset_check_result in deduplicated_results:
            asset_key = asset_check_result.asset_key
            if asset_key not in asset_check_results_by_key:
                asset_check_results_by_key[asset_key] = []
            asset_check_results_by_key[asset_key].append(asset_check_result)

        # Create MaterializeResult (skip affected downstream assets)
        for asset_key in ordered_asset_keys:
            if asset_key in affected_downstream:
                # Skip affected downstream assets - don't yield MaterializeResult
                if self._logger:
                    self._logger.warning(
                        f"Skipping materialization of {asset_key} due to upstream failures: "
                        f"{[str(key) for key in failed_asset_keys]}"
                    )
                # Raise custom exception that will be handled gracefully by factory
                raise UpstreamAuditFailureError(
                    f"Asset {asset_key} skipped due to upstream audit failures: "
                    f"{[str(key) for key in failed_asset_keys]}"
                )
            else:
                # Normal MaterializeResult for unaffected assets
                snapshot = assetkey_to_snapshot.get(asset_key)
                if snapshot:
                    snapshot_version = getattr(snapshot, "version", None)
                    model_partitions = get_model_partitions_from_plan(
                        plan, self.translator, asset_key, snapshot
                    )
                    # Prepare base metadata
                    metadata = {
                        "dagster-sqlmesh/snapshot_version": snapshot_version,
                        "dagster-sqlmesh/snapshot_timestamp": convert_unix_timestamp_to_readable(
                            getattr(snapshot, "created_ts", None)
                        )
                        if snapshot
                        else None,
                        "dagster-sqlmesh/model_name": asset_key.path[-1]
                        if asset_key.path
                        else None,
                    }

                    # Add partition metadata if model is partitioned
                    if model_partitions and model_partitions.get(
                        "is_partitioned", False
                    ):
                        partition_metadata = format_partition_metadata(model_partitions)
                        # Ensure partition metadata is compatible with Dagster 1.11.4
                        metadata["dagster-sqlmesh/partitions"] = partition_metadata

                    # Get check results for this asset
                    check_results = asset_check_results_by_key.get(asset_key, [])

                    yield MaterializeResult(
                        asset_key=asset_key,
                        metadata=metadata,
                        data_version=DataVersion(str(snapshot_version))
                        if snapshot_version
                        else None,
                        check_results=check_results,
                    )

        # End notifier-only path
