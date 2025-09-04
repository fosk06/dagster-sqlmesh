"""
EnhancedContext: An extension of SQLMesh Context with enhanced functionality.
Uses metaprogramming to automatically delegate all Context methods.
"""

import typing as t
from functools import wraps
from sqlmesh import Context
from sqlmesh.core.snapshot.evaluator import SnapshotEvaluator
from sqlmesh.core.snapshot.definition import Snapshot
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils import CompletionStatus


class DryRunSnapshotEvaluator(SnapshotEvaluator):
    """SnapshotEvaluator that simulates execution without executing real SQL queries."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.simulated_executions: t.List[t.Dict[str, t.Any]] = []

    def _evaluate_snapshot(
        self,
        start: TimeLike,
        end: TimeLike,
        execution_time: TimeLike,
        snapshot: Snapshot,
        snapshots: t.Dict[str, Snapshot],
        allow_destructive_snapshots: t.Set[str],
        allow_additive_snapshots: t.Set[str],
        deployability_index: t.Optional[t.Any],
        batch_index: int,
        target_table_exists: t.Optional[bool],
        **kwargs: t.Any,
    ) -> t.Optional[str]:
        """Override to avoid SQL execution and state updates."""

        if not snapshot.is_model:
            return None

        # Record the simulation
        simulation = {
            "snapshot_name": snapshot.name,
            "start": start,
            "end": end,
            "execution_time": execution_time,
            "batch_index": batch_index,
            "action": "would_execute",
        }
        self.simulated_executions.append(simulation)

        # SIMULATION: Just pretend to execute
        print(f"🔍 DRY-RUN: Would execute {snapshot.name}")

        # Return a simulated hash (not a real WAP ID)
        return f"dry_run_hash_{snapshot.name}_{batch_index}"

    def get_dry_run_summary(self) -> t.Dict[str, t.Any]:
        """Returns a summary of what would have been executed."""
        successful = [
            s for s in self.simulated_executions if s.get("action") == "would_execute"
        ]
        failed = [
            s for s in self.simulated_executions if s.get("action") == "would_fail"
        ]

        return {
            "total_simulated": len(self.simulated_executions),
            "would_execute": len(successful),
            "would_fail": len(failed),
            "successful_models": [s["snapshot_name"] for s in successful],
            "failed_models": [s["snapshot_name"] for s in failed],
            "executions": self.simulated_executions,
        }

    def clear_simulation(self):
        """Reset the simulations."""
        self.simulated_executions.clear()


class EnhancedContext:
    """
    SQLMesh Context with enhanced functionality.

    Uses metaprogramming to automatically delegate all Context methods
    while adding features like dry_run().
    """

    def __init__(self, context: Context):
        self._context = context
        self._dry_run_evaluator: t.Optional[DryRunSnapshotEvaluator] = None
        self._method_cache = {}

        # No automatic method creation to avoid initialization issues

    def __getattr__(self, name: str) -> t.Any:
        """Automatically delegates all undefined methods to Context with caching."""

        # Check cache first
        if name in self._method_cache:
            return self._method_cache[name]

        # If it's a Context method
        if hasattr(self._context, name):
            attr = getattr(self._context, name)

            if callable(attr):
                # Wrap the method
                @wraps(attr)
                def wrapped_method(*args, **kwargs):
                    return attr(*args, **kwargs)

                # Cache the method
                self._method_cache[name] = wrapped_method
                return wrapped_method

            # Cache the attribute
            self._method_cache[name] = attr
            return attr

        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    @property
    def dry_run_evaluator(self) -> DryRunSnapshotEvaluator:
        """Lazy creation of the dry-run evaluator."""
        if not self._dry_run_evaluator:
            # Handle the case where ddl_concurrent_tasks doesn't exist
            ddl_concurrent_tasks = getattr(
                self._context.config, "ddl_concurrent_tasks", 1
            )

            self._dry_run_evaluator = DryRunSnapshotEvaluator(
                adapters=self._context.engine_adapter,
                ddl_concurrent_tasks=ddl_concurrent_tasks,
            )
        return self._dry_run_evaluator

    def dry_run(
        self,
        environment: t.Optional[str] = None,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        skip_janitor: bool = False,
        ignore_cron: bool = False,
        select_models: t.Optional[t.Collection[str]] = None,
        exit_on_env_update: t.Optional[int] = None,
        no_auto_upstream: bool = False,
    ) -> t.Tuple[CompletionStatus, t.Dict[str, t.Any]]:
        """
        SQLMesh run dry-run: performs the entire process EXCEPT SQL execution.

        Same signature as run() but also returns the dry-run summary.

        Args:
            Same arguments as Context.run()

        Returns:
            Tuple[CompletionStatus, Dict]: (status, dry_run_summary)
        """
        print("🔍 Starting SQLMesh dry-run...")

        # Reset the dry-run evaluator
        self.dry_run_evaluator.clear_simulation()

        try:
            # Use the same logic as run() but with our evaluator
            environment = environment or self._context.config.default_target_environment

            # Create the scheduler with our dry-run evaluator
            scheduler = self._context.scheduler(
                environment=environment, snapshot_evaluator=self.dry_run_evaluator
            )

            # Execute the "run" with simulation
            completion_status = scheduler.run(
                environment=environment,
                start=start,
                end=end,
                execution_time=execution_time,
                ignore_cron=ignore_cron,
                selected_snapshots=set(select_models) if select_models else None,
                auto_restatement_enabled=environment.lower() == "prod",
                run_environment_statements=False,  # No env statements in dry-run
            )

            # Get the summary
            dry_run_summary = self.dry_run_evaluator.get_dry_run_summary()

            print(f"🎯 Dry-run completed: {completion_status}")
            print(f"📊 Would execute {dry_run_summary['would_execute']} models")

            return completion_status, dry_run_summary

        except Exception as e:
            print(f"❌ Dry-run failed: {e}")
            dry_run_summary = self.dry_run_evaluator.get_dry_run_summary()
            return CompletionStatus.FAILURE, dry_run_summary

    def will_run_execute_models(
        self,
        environment: t.Optional[str] = None,
        select_models: t.Optional[t.Collection[str]] = None,
        **dry_run_kwargs,
    ) -> bool:
        """
        Utility method to know if run() will execute models.

        Returns:
            bool: True if models will be executed, False otherwise
        """
        completion_status, _ = self.dry_run(
            environment=environment, select_models=select_models, **dry_run_kwargs
        )

        return not completion_status.is_nothing_to_do

    def get_models_to_execute(
        self,
        environment: t.Optional[str] = None,
        select_models: t.Optional[t.Collection[str]] = None,
        **dry_run_kwargs,
    ) -> t.List[str]:
        """
        Utility method to get the list of models that would be executed.

        Returns:
            List[str]: List of model names that would be executed
        """
        completion_status, dry_run_summary = self.dry_run(
            environment=environment, select_models=select_models, **dry_run_kwargs
        )

        if completion_status.is_nothing_to_do:
            return []

        return dry_run_summary.get("successful_models", [])
