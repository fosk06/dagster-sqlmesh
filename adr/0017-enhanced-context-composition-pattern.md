# ADR 0017: EnhancedContext Composition Pattern with Metaprogramming

## Status

Accepted

## Context

Dans le cadre de l'intégration SQLMesh-Dagster, nous avons besoin d'étendre les fonctionnalités du `Context` SQLMesh avec des capacités de dry-run pour optimiser l'exécution des schedules. Cette extension doit permettre de :

1. **Simuler l'exécution SQLMesh** sans exécuter de vraies requêtes SQL
2. **Détecter les modèles à exécuter** avant l'exécution réelle
3. **Éviter les runs à vide** dans les schedules Dagster
4. **Maintenir la compatibilité** avec toutes les méthodes existantes du `Context` SQLMesh

## Decision

Nous avons choisi d'implémenter `EnhancedContext` en utilisant **la composition avec métaprogramming** plutôt que l'héritage direct de `sqlmesh.core.context.Context`.

### Architecture Choisie

```python
class EnhancedContext:
    """
    Context SQLMesh avec fonctionnalités étendues.
    
    Utilise la métaprogrammation pour déléguer automatiquement toutes les méthodes
    de Context tout en ajoutant des fonctionnalités comme dry_run().
    """
    
    def __init__(self, context: Context):
        self._context = context
        self._method_cache = {}
    
    def __getattr__(self, name: str) -> t.Any:
        """Délègue automatiquement toutes les méthodes non définies à Context avec cache."""
        # Vérifier le cache d'abord
        if name in self._method_cache:
            return self._method_cache[name]
        
        # Si c'est une méthode de Context
        if hasattr(self._context, name):
            attr = getattr(self._context, name)
            # Wrapper et cache la méthode
            self._method_cache[name] = attr
            return attr
        
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
```

## Rationale

### Pourquoi la Composition ?

1. **Robustesse contre les changements d'API** : SQLMesh peut évoluer et ajouter de nouvelles méthodes. Avec la composition, ces méthodes sont automatiquement disponibles sans modification de notre code.

2. **Éviter les problèmes d'initialisation** : L'héritage direct de `Context` déclenche des vérifications de version SQLMesh lors de l'initialisation, causant des erreurs dans certains environnements de test.

3. **Flexibilité** : Nous pouvons facilement ajouter, modifier ou supprimer des fonctionnalités sans affecter le comportement de base du `Context`.

4. **Séparation des responsabilités** : `EnhancedContext` se concentre sur ses nouvelles fonctionnalités (dry-run) tandis que le `Context` original gère les fonctionnalités SQLMesh de base.

### Pourquoi la Métaprogramming ?

1. **Délégation automatique** : `__getattr__` permet de déléguer automatiquement toutes les méthodes non définies au `Context` sous-jacent.

2. **Performance** : Le cache des méthodes évite les appels répétés à `hasattr` et `getattr`.

3. **Transparence** : L'API reste identique à celle du `Context` original, garantissant la compatibilité.

4. **Lazy loading** : Les méthodes ne sont déléguées que lorsqu'elles sont appelées, évitant les problèmes d'initialisation prématurée.

### Alternatives Considérées

#### 1. Héritage Direct
```python
class EnhancedContext(Context):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Problème : déclenche les vérifications de version SQLMesh
```

**Problèmes :**
- Déclenche les vérifications de version SQLMesh lors de l'initialisation
- Fragile aux changements d'API SQLMesh
- Couplage fort avec l'implémentation interne de SQLMesh

#### 2. Composition Simple sans Métaprogramming
```python
class EnhancedContext:
    def __init__(self, context: Context):
        self._context = context
    
    def plan(self, *args, **kwargs):
        return self._context.plan(*args, **kwargs)
    
    def run(self, *args, **kwargs):
        return self._context.run(*args, **kwargs)
    # ... répéter pour toutes les méthodes
```

**Problèmes :**
- Code répétitif et maintenu manuellement
- Nécessite de connaître toutes les méthodes de `Context`
- Fragile aux ajouts de nouvelles méthodes dans SQLMesh

#### 3. Proxy Pattern avec inspect
```python
def _create_delegated_methods(self):
    for name, method in inspect.getmembers(self._context, callable):
        setattr(self, name, method)
```

**Problèmes :**
- Déclenche les vérifications SQLMesh lors de l'initialisation
- Moins flexible que `__getattr__`
- Cache statique qui ne s'adapte pas aux changements

## Consequences

### Positives

1. **Robustesse** : Résistant aux changements d'API SQLMesh
2. **Performance** : Cache des méthodes pour éviter les appels répétés
3. **Maintenabilité** : Code simple et facile à comprendre
4. **Compatibilité** : API identique au `Context` original
5. **Flexibilité** : Facile d'ajouter de nouvelles fonctionnalités

### Négatives

1. **Complexité conceptuelle** : La métaprogramming peut être difficile à comprendre pour les nouveaux développeurs
2. **Debugging** : Les erreurs de délégation peuvent être moins évidentes
3. **Documentation** : Nécessite une documentation claire pour expliquer le pattern

### Mitigations

1. **Documentation détaillée** : Commentaires et docstrings explicites
2. **Tests complets** : Couverture de tous les cas d'usage
3. **Logging** : Logs pour tracer les délégations si nécessaire
4. **Type hints** : Annotations de type pour améliorer l'IDE support

## Implementation Details

### Cache des Méthodes

Le cache `_method_cache` évite les appels répétés à `hasattr` et `getattr` :

```python
def __getattr__(self, name: str) -> t.Any:
    # Vérifier le cache d'abord
    if name in self._method_cache:
        return self._method_cache[name]
    
    # Délégation et mise en cache
    if hasattr(self._context, name):
        attr = getattr(self._context, name)
        self._method_cache[name] = attr
        return attr
```

### Gestion des Erreurs

Les erreurs d'attribut sont propagées avec des messages clairs :

```python
raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
```

### Compatibilité avec les Wrappers

Les méthodes peuvent être wrappées si nécessaire :

```python
if callable(attr):
    @wraps(attr)
    def wrapped_method(*args, **kwargs):
        return attr(*args, **kwargs)
    self._method_cache[name] = wrapped_method
    return wrapped_method
```

## Related ADRs

- [ADR 0002: Shared SQLMesh Execution](../adr/0002-shared-sqlmesh-execution.md) - Contexte sur l'exécution SQLMesh partagée
- [ADR 0007: SQLMesh Plan Run Flow](../adr/0007-sqlmesh-plan-run-flow.md) - Flux d'exécution SQLMesh
- [ADR 0010: Error Handling Strategy](../adr/0010-error-handling-strategy.md) - Stratégie de gestion d'erreurs

## References

- [SQLMesh Context Documentation](https://sqlmesh.readthedocs.io/en/stable/concepts/context/)
- [Python Metaprogramming Guide](https://docs.python.org/3/reference/datamodel.html#object.__getattr__)
- [Composition over Inheritance Principle](https://en.wikipedia.org/wiki/Composition_over_inheritance)
