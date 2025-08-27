# ADR-0016: SQLMesh Model Skip Detection and AssetCheck Integration

## Status

**Accepted** - 2025-08-27

## Context

### Problem Statement

SQLMesh peut "skipper" des modèles pour plusieurs raisons :
1. **Cron-based skips** : Le modèle a un cron `@daily` mais il n'est pas encore l'heure
2. **No changes detected** : SQLMesh détermine qu'aucun changement n'est nécessaire
3. **Upstream failures** : Les modèles en amont ont échoué

**Avant** : Tous les modèles retournaient un `MaterializeResult` avec statut "success", même ceux qui n'étaient pas réellement exécutés.

**Problème** : L'utilisateur ne pouvait pas distinguer entre :
- ✅ Modèle réellement exécuté et recalculé
- ⚠️ Modèle skippé (normal, attendu)
- ❌ Modèle en échec

### Requirements

1. **Distinction claire** entre modèles exécutés et skippés
2. **Feedback utilisateur** via l'interface Dagster
3. **Intégration** avec le système AssetCheck existant
4. **Non-bloquant** : Les skips ne doivent pas arrêter l'exécution

## Decision

### Solution Chosen: AssetCheck "sqlmesh_execution_status"

Nous avons implémenté un **AssetCheck automatique** nommé `sqlmesh_execution_status` qui est ajouté à tous les modèles SQLMesh et qui indique leur statut d'exécution réel.

### Architecture

```
┌─────────────────┐    ┌──────────────────────┐    ┌─────────────────┐
│   SQLMesh Run  │    │   SimpleRunTracker   │    │   AssetCheck    │
│                 │───▶│   (execution log)    │───▶│   Results       │
└─────────────────┘    └──────────────────────┘    └─────────────────┘
         │                        │                        │
         ▼                        ▼                        ▼
   Models Executed         Models Skipped           Dagster UI
   (tracked via            (deduced from            (SUCCESS vs
    update_snapshot_        requested - executed)    WARNING)
    evaluation_progress)
```

### Key Components

#### 1. Automatic AssetCheck Creation
```python
def create_asset_checks_from_model(model: Any, asset_key: AssetKey) -> List[AssetCheckSpec]:
    """Creates AssetCheckSpec for audits of a SQLMesh model."""
    asset_checks = []
    
    # ... existing audit checks ...
    
    # Add automatic execution status check for ALL models
    asset_checks.append(
        AssetCheckSpec(
            name="sqlmesh_execution_status",
            asset=asset_key,
            description=f"SQLMesh execution status for model {model.name}",
            blocking=False,  # This is informational only
            metadata={
                "sqlmesh_model": model.name,
                "check_type": "execution_status",
            },
        )
    )
    
    return asset_checks
```

#### 2. Execution Status Logic
```python
def handle_successful_execution(context, current_model_name, current_asset_spec, 
                              current_model_checks, sqlmesh_executed_models, ...):
    """Handle the case where the model executed successfully."""
    
    # Check if this model was executed by SQLMesh
    model_was_executed_by_sqlmesh = current_model_name in sqlmesh_executed_models
    
    # Find the execution status check spec
    execution_status_check = next(
        (check for check in current_model_checks 
         if check.name == "sqlmesh_execution_status"),
        None
    )
    
    if execution_status_check:
        if model_was_executed_by_sqlmesh:
            # Model was executed → SUCCESS
            check_results.append(
                AssetCheckResult(
                    passed=True,
                    severity=AssetCheckSeverity.WARN,  # Use WARN since INFO doesn't exist
                    check_name="sqlmesh_execution_status",
                    metadata={
                        "sqlmesh_model": current_model_name,
                        "check_type": "execution_status",
                        "status": "executed",
                        "message": f"Model {current_model_name} was executed by SQLMesh",
                    },
                )
            )
        else:
            # Model was skipped → WARNING (user should know)
            check_results.append(
                AssetCheckResult(
                    passed=False,
                    severity=AssetCheckSeverity.WARN,
                    check_name="sqlmesh_execution_status",
                    metadata={
                        "sqlmesh_model": current_model_name,
                        "check_type": "execution_status",
                        "status": "skipped",
                        "message": f"Model {current_model_name} was skipped by SQLMesh",
                    },
                )
            )
```

#### 3. Skip Detection Strategy
```python
def execute_sqlmesh_materialization(context, sqlmesh, sqlmesh_results, run_id, selected_asset_keys):
    """Execute SQLMesh materialization with execution tracking."""
    
    # Get all requested models
    requested_models = [model.name for model in models_to_materialize]
    
    with sqlmesh_run_tracker(sqlmesh.context) as tracker:
        # SQLMesh execution happens here
        plan = sqlmesh.materialize_assets_threaded(models_to_materialize, context)
        
        # Get executed models from tracker
        results = tracker.get_results()
        sqlmesh_executed_models = results['run_models']
        
        # DEDUCE skipped models: requested - executed
        normalized_executed_models = [
            _parse_snapshot_to_model_name(name) 
            for name in sqlmesh_executed_models
        ]
        sqlmesh_skipped_models = list(
            set(requested_models) - set(normalized_executed_models)
        )
    
    # Store results for later use
    results_payload = {
        # ... existing fields ...
        "sqlmesh_executed_models": normalized_executed_models,
        "sqlmesh_skipped_models": sqlmesh_skipped_models,
    }
```

## Consequences

### Positive

1. **Transparence totale** : L'utilisateur voit exactement ce qui s'est passé
2. **Non-bloquant** : Les skips n'arrêtent pas l'exécution
3. **Intégration native** : Utilise le système AssetCheck Dagster existant
4. **Automatique** : Pas besoin de configuration utilisateur
5. **Consistent** : Même interface pour tous les modèles

### Negative

1. **Complexité** : Logique de déduction des modèles skippés
2. **Dépendance** : Sur le bon fonctionnement du SimpleRunTracker
3. **UI confusion** : Plus de checks à gérer dans l'interface Dagster

### Risks and Mitigations

#### Risk: Incorrect Skip Detection
- **Mitigation** : Tests unitaires complets, validation des résultats
- **Monitoring** : Logs détaillés de la déduction

#### Risk: Performance Impact
- **Mitigation** : Sets Python optimisés, pas de requêtes supplémentaires
- **Monitoring** : Mesure des temps d'exécution

## Implementation Details

### File Structure
```
src/dg_sqlmesh/
├── sqlmesh_asset_check_utils.py      # AssetCheck creation with sqlmesh_execution_status
├── sqlmesh_asset_execution_utils.py  # Skip detection logic
└── simple_run_tracker.py              # Execution tracking
```

### Data Flow
```
1. SQLMesh Run Request
   ↓
2. SimpleRunTracker Injection
   ↓
3. SQLMesh Execution (with tracking)
   ↓
4. Results Collection
   ↓
5. Skip Detection (requested - executed)
   ↓
6. AssetCheck Results Creation
   ↓
7. Dagster UI Display
```

### UI Behavior

#### SUCCESS Check (Model Executed)
- **Color** : Vert ✅
- **Message** : "Model was executed by SQLMesh"
- **Status** : Passed

#### WARNING Check (Model Skipped)
- **Color** : Orange ⚠️
- **Message** : "Model was skipped by SQLMesh"
- **Status** : Failed (but non-blocking)

### Skip Detection Logic

#### 1. Direct Tracking
- **Executed models** : Capturés via `update_snapshot_evaluation_progress`
- **Explicit skips** : Capturés via `log_skipped_models` (upstream failures)

#### 2. Deduction Logic
- **Cron-based skips** : Déduits par `requested_models - executed_models`
- **No-changes skips** : Déduits par `requested_models - executed_models`

#### 3. Normalization
```python
def _parse_snapshot_to_model_name(snapshot_name: str) -> str | None:
    """Convert '"db"."schema"."model"' to 'schema.model'."""
    try:
        parts = snapshot_name.split('"."')
        if len(parts) >= 3:
            return parts[1] + "." + parts[2].replace('"', "")
    except Exception:
        return None
    return None
```

## Alternatives Considered

### 1. AssetObservation for Skipped Models
- **Approach** : Retourner `AssetObservation` au lieu de `MaterializeResult`
- **Rejection** : Trop complexe, cas edge nombreux, maintenance difficile

### 2. Custom Metadata in MaterializeResult
- **Approach** : Ajouter un champ `execution_status` dans les métadonnées
- **Rejection** : Moins visible pour l'utilisateur, pas d'intégration native Dagster

### 3. Separate AssetCheck for Each Skip Reason
- **Approach** : Créer des checks spécifiques (`cron_skip`, `no_changes_skip`)
- **Rejection** : Trop granulaire, complexité excessive

## Related ADRs

- [ADR-0015: Custom SQLMesh Console for Model Execution Tracking](./0015-custom-sqlmesh-console-execution-tracking.md)
- [ADR-0003: Asset Check Integration](./0003-asset-check-integration.md)
- [ADR-0007: SQLMesh Plan Run Flow](./0007-sqlmesh-plan-run-flow.md)

## References

- [Dagster AssetCheck Documentation](https://docs.dagster.io/concepts/assets/asset-checks)
- [SQLMesh Execution Model](https://sqlmesh.readthedocs.io/en/stable/concepts/execution.html)
- [Python Set Operations](https://docs.python.org/3/library/stdtypes.html#set-types-set-frozenset)
