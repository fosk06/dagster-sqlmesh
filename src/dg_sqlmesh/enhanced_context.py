"""
EnhancedContext : Une extension de Context SQLMesh avec fonctionnalités étendues.
Utilise la métaprogrammation pour déléguer automatiquement toutes les méthodes de Context.
"""

import typing as t
from functools import wraps
from sqlmesh import Context
from sqlmesh.core.snapshot.evaluator import SnapshotEvaluator
from sqlmesh.core.snapshot.definition import Snapshot
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils import CompletionStatus


class DryRunSnapshotEvaluator(SnapshotEvaluator):
    """SnapshotEvaluator qui simule l'exécution sans faire les vraies requêtes SQL."""

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
        """Surcharge pour éviter l'exécution SQL et la mise à jour du state."""

        if not snapshot.is_model:
            return None

        # Enregistrer la simulation
        simulation = {
            "snapshot_name": snapshot.name,
            "start": start,
            "end": end,
            "execution_time": execution_time,
            "batch_index": batch_index,
            "action": "would_execute",
        }
        self.simulated_executions.append(simulation)

        # SIMULATION : On fait juste semblant d'exécuter
        print(f"🔍 DRY-RUN: Would execute {snapshot.name}")

        # Retourner un hash simulé (pas de WAP ID réel)
        return f"dry_run_hash_{snapshot.name}_{batch_index}"

    def get_dry_run_summary(self) -> t.Dict[str, t.Any]:
        """Retourne un résumé de ce qui aurait été exécuté."""
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
        """Remet à zéro les simulations."""
        self.simulated_executions.clear()


class EnhancedContext:
    """
    Context SQLMesh avec fonctionnalités étendues.

    Utilise la métaprogrammation pour déléguer automatiquement toutes les méthodes
    de Context tout en ajoutant des fonctionnalités comme dry_run().
    """

    def __init__(self, context: Context):
        self._context = context
        self._dry_run_evaluator: t.Optional[DryRunSnapshotEvaluator] = None
        self._method_cache = {}

        # Pas de création automatique de méthodes pour éviter les problèmes d'initialisation

    def __getattr__(self, name: str) -> t.Any:
        """Délègue automatiquement toutes les méthodes non définies à Context avec cache."""

        # Vérifier le cache d'abord
        if name in self._method_cache:
            return self._method_cache[name]

        # Si c'est une méthode de Context
        if hasattr(self._context, name):
            attr = getattr(self._context, name)

            if callable(attr):
                # Wrapper la méthode
                @wraps(attr)
                def wrapped_method(*args, **kwargs):
                    return attr(*args, **kwargs)

                # Mettre en cache
                self._method_cache[name] = wrapped_method
                return wrapped_method

            # Mettre en cache l'attribut
            self._method_cache[name] = attr
            return attr

        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    @property
    def dry_run_evaluator(self) -> DryRunSnapshotEvaluator:
        """Lazy création du dry-run evaluator."""
        if not self._dry_run_evaluator:
            # Gérer le cas où ddl_concurrent_tasks n'existe pas
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
        Dry-run de sqlmesh run : fait tout le processus SAUF l'exécution SQL.

        Même signature que run() mais retourne aussi le résumé du dry-run.

        Args:
            Mêmes arguments que Context.run()

        Returns:
            Tuple[CompletionStatus, Dict]: (status, dry_run_summary)
        """
        print("🔍 Starting SQLMesh dry-run...")

        # Reset du dry-run evaluator
        self.dry_run_evaluator.clear_simulation()

        try:
            # Utiliser la même logique que run() mais avec notre evaluator
            environment = environment or self._context.config.default_target_environment

            # Créer le scheduler avec notre dry-run evaluator
            scheduler = self._context.scheduler(
                environment=environment, snapshot_evaluator=self.dry_run_evaluator
            )

            # Exécuter le "run" avec simulation
            completion_status = scheduler.run(
                environment=environment,
                start=start,
                end=end,
                execution_time=execution_time,
                ignore_cron=ignore_cron,
                selected_snapshots=set(select_models) if select_models else None,
                auto_restatement_enabled=environment.lower() == "prod",
                run_environment_statements=False,  # Pas de statements d'env en dry-run
            )

            # Récupérer le résumé
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
        Méthode utilitaire pour savoir si run() va exécuter des modèles.

        Returns:
            bool: True si des modèles vont être exécutés, False sinon
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
        Méthode utilitaire pour obtenir la liste des modèles qui seraient exécutés.

        Returns:
            List[str]: Liste des noms de modèles qui seraient exécutés
        """
        completion_status, dry_run_summary = self.dry_run(
            environment=environment, select_models=select_models, **dry_run_kwargs
        )

        if completion_status.is_nothing_to_do:
            return []

        return dry_run_summary.get("successful_models", [])
