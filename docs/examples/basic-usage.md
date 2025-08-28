# Basic Usage Examples

This page provides practical examples of how to use dg-sqlmesh in common scenarios.

## Simple Integration

The most basic usage involves creating a simple factory configuration:

```python
from dg_sqlmesh import sqlmesh_definitions_factory

# Basic configuration
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    gateway="postgres",
    concurrency_limit=1,
    group_name="sqlmesh",
)
```

## With External Asset Mapping

Add external asset mapping for sources from other systems:

```python
from dg_sqlmesh import sqlmesh_definitions_factory

# With external asset mapping
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    gateway="postgres",
    external_asset_mapping="raw/{node.name}",
    concurrency_limit=1,
    group_name="sqlmesh",
    enable_schedule=True,
)
```

## Custom Configuration

For more control, configure individual components:

```python
from dagster import Definitions, RetryPolicy, Backoff
from dg_sqlmesh import sqlmesh_assets_factory, sqlmesh_adaptive_schedule_factory
from dg_sqlmesh import SQLMeshResource

# SQLMesh resource
sqlmesh_resource = SQLMeshResource(
    project_dir="sqlmesh_project",
    gateway="postgres",
    concurrency_limit=1,
)

# SQLMesh assets
sqlmesh_assets = sqlmesh_assets_factory(
    sqlmesh_resource=sqlmesh_resource,
    group_name="sqlmesh",
    op_tags={"team": "data", "env": "prod"},
    retry_policy=RetryPolicy(max_retries=1, delay=30.0, backoff=Backoff.EXPONENTIAL),
)

# Adaptive schedule
sqlmesh_adaptive_schedule, sqlmesh_job, _ = sqlmesh_adaptive_schedule_factory(
    sqlmesh_resource=sqlmesh_resource
)

# Combine everything
defs = Definitions(
    assets=[sqlmesh_assets],
    jobs=[sqlmesh_job],
    schedules=[sqlmesh_adaptive_schedule],
    resources={
        "sqlmesh": sqlmesh_resource,
    },
)
```

## Custom Translator

Create custom asset key mapping:

```python
from dg_sqlmesh import SQLMeshTranslator
from dagster import AssetKey

class CustomTranslator(SQLMeshTranslator):
    def get_external_asset_key(self, external_fqn: str) -> AssetKey:
        """Custom mapping for external assets."""
        parts = external_fqn.replace('"', '').split('.')
        if len(parts) >= 3:
            catalog, schema, table = parts[0], parts[1], parts[2]
            return AssetKey(['raw', schema, table])
        return AssetKey(['external'] + parts[1:])

# Use custom translator
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    gateway="postgres",
    translator=CustomTranslator(),
    concurrency_limit=1,
)
```

## Component Usage

Use the declarative YAML component:

```yaml
# defs.yaml
type: dg_sqlmesh.SQLMeshProjectComponent

attributes:
  sqlmesh_config:
    project_path: "{{ project_root }}/sqlmesh_project"
    gateway: "postgres"
    environment: "prod"
  concurrency_jobs_limit: 1
  default_group_name: "sqlmesh"
  op_tags:
    team: "data"
    env: "prod"
  external_asset_mapping: "target/main/{node.name}"
```

## Testing

Test your integration locally:

```python
# test_integration.py
from dagster import build_asset_context
from dg_sqlmesh import sqlmesh_definitions_factory

# Create definitions
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    gateway="duckdb",  # Use DuckDB for testing
    concurrency_limit=1,
)

# Test individual asset
context = build_asset_context()
asset_def = defs.get_asset_def("your_model_name")
result = asset_def.compute_fn(context)

print(f"Asset result: {result}")
```

## Next Steps

- **[Advanced Usage](advanced-usage.md)** - Complex scenarios and patterns
- **[API Reference](../api/factory-functions.md)** - Complete function documentation
- **[User Guide](../user-guide/core-concepts.md)** - Deep dive into concepts
