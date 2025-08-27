# ADR-0015: Custom SQLMesh Console for Model Execution Tracking

## Status

**Accepted** - 2025-08-27

## Context

### Problem Statement

Dans notre architecture Dagster-SQLMesh, nous devons savoir quels modèles SQLMesh sont réellement exécutés vs ceux qui sont skippés (par exemple à cause des crons ou de l'absence de changements).

**Avant** : On retournait toujours un `MaterializeResult` pour tous les modèles sélectionnés, même ceux que SQLMesh n'exécutait pas réellement.

**Problème** : Cela induisait l'utilisateur en erreur en lui faisant croire que tous les modèles avaient été recalculés.

### Requirements

1. **Détection précise** des modèles exécutés par SQLMesh
2. **Identification** des modèles skippés (cron, pas de changements)
3. **Intégration transparente** avec l'architecture Dagster existante
4. **Performance** : pas d'impact sur l'exécution SQLMesh

## Decision

### Solution Chosen: Custom SQLMesh Console Injection

Nous avons implémenté un **custom console SQLMesh** (`SimpleRunTracker`) qui s'injecte dans `sqlmesh.context.console` pour capturer les événements d'exécution en temps réel.

### Architecture

```
┌─────────────────┐    ┌──────────────────────┐    ┌─────────────────┐
│   Dagster      │    │   SQLMesh Context    │    │  SimpleRunTracker│
│   Execution    │───▶│   (injected console) │───▶│  (our custom    │
│                 │    │                      │    │   console)      │
└─────────────────┘    └──────────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   SQLMesh Run   │
                       │   (execution)   │
                       └─────────────────┘
```

### Key Components

#### 1. SimpleRunTracker

```python
class SimpleRunTracker(Console):
    """Custom SQLMesh console to track executed vs skipped models."""

    def __init__(self):
        self.run_models: Set[str] = set()      # Models actually executed
        self.skipped_models: Set[str] = set()  # Models explicitly skipped

    def update_snapshot_evaluation_progress(self, snapshot: Snapshot, audit_only: bool = False):
        """Called by SQLMesh when a model is executed."""
        self.run_models.add(snapshot.name)

    def log_skipped_models(self, snapshot_names: Set[str]):
        """Called by SQLMesh when models are skipped (upstream failures)."""
        self.skipped_models.update(snapshot_names)
```

#### 2. Context Manager Integration

```python
@contextmanager
def sqlmesh_run_tracker(sqlmesh_context):
    """Inject our custom console into SQLMesh context."""
    original_console = sqlmesh_context.console
    tracker = SimpleRunTracker()
    sqlmesh_context.console = tracker

    try:
        yield tracker
    finally:
        sqlmesh_context.console = original_console  # Restore original
```

#### 3. Execution Flow

```python
def execute_sqlmesh_materialization(context, sqlmesh, sqlmesh_results, run_id, selected_asset_keys):
    # ... existing logic ...

    with sqlmesh_run_tracker(sqlmesh.context) as tracker:
        # SQLMesh execution happens here
        plan = sqlmesh.materialize_assets_threaded(models_to_materialize, context)

        # Capture results from our tracker
        results = tracker.get_results()
        sqlmesh_executed_models = results['run_models']
        sqlmesh_skipped_models = results['skipped_models']

    # Store results for later use
    results_payload = {
        # ... existing fields ...
        "sqlmesh_executed_models": sqlmesh_executed_models,
        "sqlmesh_skipped_models": sqlmesh_skipped_models,
    }
```

## Consequences

### Positive

1. **Précision** : On sait exactement quels modèles ont été exécutés
2. **Transparence** : L'utilisateur voit le vrai statut d'exécution
3. **Non-intrusif** : Pas de modification de l'API SQLMesh
4. **Performance** : Overhead minimal (juste des sets Python)
5. **Robustesse** : Restauration automatique du console original

### Negative

1. **Complexité** : Ajout d'une couche d'abstraction
2. **Dépendance** : On dépend de l'API interne de SQLMesh Console
3. **Maintenance** : Risque de breakage si SQLMesh change l'interface Console

### Risks and Mitigations

#### Risk: SQLMesh Console API Changes

- **Mitigation** : Tests unitaires complets, documentation des dépendances
- **Monitoring** : Vérification lors des upgrades SQLMesh

#### Risk: Console Injection Failure

- **Mitigation** : Context manager avec restoration garantie
- **Fallback** : Logs d'erreur et comportement dégradé

## Implementation Details

### File Structure

```
src/dg_sqlmesh/
├── simple_run_tracker.py          # Custom console implementation
├── sqlmesh_asset_execution_utils.py  # Integration with execution flow
└── sqlmesh_asset_check_utils.py      # AssetCheck creation
```

### Integration Points

1. **execute_sqlmesh_materialization** : Injection du tracker
2. **handle_successful_execution** : Utilisation des résultats pour AssetCheck
3. **create_asset_checks_from_model** : Ajout automatique du check `sqlmesh_execution_status`

### Data Flow

```
SQLMesh Run → SimpleRunTracker → Results Payload → AssetCheck Results
     ↓              ↓                ↓              ↓
  Execution    Model Tracking    Stored Data    Dagster UI
```

## Alternatives Considered

### 1. SQLMesh Plan Analysis

- **Approach** : Analyser le plan SQLMesh avant exécution
- **Rejection** : Le plan ne reflète pas toujours la réalité d'exécution

### 2. Post-Execution Log Parsing

- **Approach** : Parser les logs SQLMesh après exécution
- **Rejection** : Fragile, dépendant du format des logs

### 3. SQLMesh Event Hooks

- **Approach** : Utiliser des hooks d'événements SQLMesh
- **Rejection** : API non documentée, risque de breakage

## Related ADRs

- [ADR-0007: SQLMesh Plan Run Flow](./0007-sqlmesh-plan-run-flow.md)
- [ADR-0003: Asset Check Integration](./0003-asset-check-integration.md)
- [ADR-0008: Shared State Management Pattern](./0008-shared-state-management-pattern.md)

## References

- [SQLMesh Console Interface](https://sqlmesh.readthedocs.io/en/stable/concepts/console.html)
- [Dagster AssetCheck Documentation](https://docs.dagster.io/concepts/assets/asset-checks)
- [Python Context Managers](https://docs.python.org/3/library/contextlib.html)
