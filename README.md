# SQLMesh-Dagster Integration

This module provides a complete integration between SQLMesh and Dagster, allowing SQLMesh models to be materialized as Dagster assets with support for audits, metadata, and adaptive scheduling.

## Features

### 🎯 **SQLMesh Model to Dagster Asset Conversion**

- **Individual asset control** : Each SQLMesh model becomes a separate Dagster asset with granular success/failure control
- **Automatic materialization** : SQLMesh models are automatically converted to Dagster assets
- **External assets support** : SQLMesh sources (external assets) are mapped to Dagster AssetKeys
- **🆕 Jinja2 template mapping** : Easy external asset mapping with `external_asset_mapping` parameter
- **Automatic dependencies** : Dependencies between models are preserved in Dagster
- **Partitioning** : Support for partitioned SQLMesh models (managed by SQLMesh, no integration with Dagster partitions - no Dagster → SQLMesh backfill)

### 📊 **SQLMesh Metadata Integration to Dagster**

- **Complete metadata** : Cron, tags, kind, dialect, query, partitioned_by, clustered_by
- **Code versioning** : Uses SQLMesh data_hash for Dagster versioning
- **Column descriptions** : Table metadata with descriptions
- **Customizable tags** : SQLMesh tags mapping to Dagster

### ✅ **SQLMesh Audits to Asset Checks Conversion**

- **Automatic audits** : SQLMesh audits become Dagster AssetCheckSpec
- **AssetCheckResult** : Automatic emission of audit results with proper output handling
- **Audit metadata** : SQL query, arguments, dialect, blocking status
- **Non-blocking** : Dagster checks are non-blocking (SQLMesh handles blocking)
- **Notifier-based capture** : Audit failures captured via notifier service (no console)

### ⏰ **Adaptive Scheduling**

- **Automatic analysis** : Detection of the finest granularity from SQLMesh crons
- **Adaptive schedule** : Automatic creation of a Dagster schedule based on crons
- **Intelligent execution** : SQLMesh manages which models should be executed
- **Monitoring** : Detailed logs and granularity metadata

### 🔧 **All-in-One Factory**

- **Simple configuration** : Single function to configure everything
- **Extensible translator** : Customizable translator system
- **Automatic validation** : External dependencies validation
- **Retry policy** : Centralized retry policy configuration

## SQLMesh Feature Coverage

| SQLMesh Feature                 | Status           | Dagster Integration                         | Notes                                                           |
| ------------------------------- | ---------------- | ------------------------------------------- | --------------------------------------------------------------- |
| **Model Types**                 |
| FULL models                     | ✅ Supported     | Individual assets with full materialization | Complete rebuild on each run                                    |
| INCREMENTAL models              | ✅ Supported     | Individual assets with incremental logic    | SQLMesh handles incremental logic, no Dagster partition binding |
| SEED models                     | ✅ Supported     | Individual assets for data loading          | CSV/Parquet file loading                                        |
| EXTERNAL models                 | ✅ Supported     | External asset mapping                      | Sources from other systems                                      |
| VIEW models                     | ✅ Supported     | Individual assets as views                  | Virtual tables                                                  |
| TABLE models                    | ✅ Supported     | Individual assets as tables                 | Materialized tables                                             |
| **Model Properties**            |
| Cron scheduling                 | ✅ Supported     | Adaptive schedule creation                  | Automatic cron analysis                                         |
| Tags                            | ✅ Supported     | Dagster tag mapping                         | `dagster:property:value` convention                             |
| Audits                          | ✅ Supported     | AssetCheckSpec conversion                   | Automatic audit to check mapping                                |
| Column descriptions             | ✅ Supported     | Table metadata                              | Rich schema information                                         |
| Partitioning                    | ✅ Supported     | Metadata extraction                         | SQLMesh-managed partitions                                      |
| Grain definition                | ✅ Supported     | Metadata extraction                         | Data granularity info                                           |
| **Execution Features**          |
| Plan validation                 | ✅ Supported     | Combined plan/run execution                 | Validation before materialization                               |
| Run execution                   | ✅ Supported     | Materialization orchestration               | Single SQLMesh run per Dagster run                              |
| Environment management          | ❌ Not supported | External responsibility                     | CLI/CI-CD managed                                               |
| Breaking changes                | ❌ Not supported | External responsibility                     | CLI/CI-CD managed                                               |
| **Advanced Features**           |
| Multi-dialect support           | ✅ Supported     | Dialect metadata                            | PostgreSQL, DuckDB, etc.                                        |
| Custom macros                   | ✅ Supported     | SQL execution                               | Full SQLMesh macro support                                      |
| Model dependencies              | ✅ Supported     | Dagster dependency graph                    | Automatic dependency resolution                                 |
| Audit blocking                  | ✅ Supported     | Non-blocking checks                         | SQLMesh handles blocking logic                                  |
| **Future Features**             |
| Non-blocking audits             | ✅ Supported     | AssetCheckResult with WARN severity         | Recognizes `_non_blocking` suffix; SQLMesh manages blocking     |
| Dagster → SQLMesh backfill      | 🔄 Planned       | Partition integration                       | Direct Dagster partition control                                |
| Multi-environment orchestration | ❌ Not supported | Dagster OSS does not support multi-tenancy  | Use separate Dagster clusters per environment                   |
| **Dagster-Specific Features**   |
| Dagster Component packaging     | ✅ Supported     | Standalone Dagster component                | Package as reusable Dagster (yaml DSL) component                |
| Custom asset groups             | ✅ Supported     | Automatic group assignment                  | Based on model path and tags                                    |
| Asset selection & filtering     | ✅ Supported     | Selective materialization                   | Materialize specific models or groups                           |
| Dagster UI integration          | ✅ Supported     | Individual asset visibility                 | Each model visible as separate asset in UI                      |
| Asset check results             | ✅ Supported     | Audit results as AssetCheckResult           | SQLMesh audits converted to Dagster checks                      |
| Custom translators              | ✅ Supported     | Extensible translator system                | Custom mapping for external assets and metadata                 |
| Shared execution optimization   | ✅ Supported     | Single SQLMesh run per Dagster run          | SQLMeshResultsResource for shared state                         |
| Adaptive scheduling             | ✅ Supported     | Automatic schedule creation                 | Based on SQLMesh cron analysis                                  |

## Basic Usage

### **Simple Factory (Recommended)**

```python
from dagster import RetryPolicy, AssetKey, Backoff
from dg_sqlmesh import sqlmesh_definitions_factory

# All-in-one factory with external asset mapping!
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    gateway="postgres",
    external_asset_mapping="target/main/{node.name}",  # 🆕 NEW: Jinja2 template for external assets
    concurrency_limit=1,
    group_name="sqlmesh",
    op_tags={"team": "data", "env": "prod"},
    retry_policy=RetryPolicy(max_retries=1, delay=30.0, backoff=Backoff.EXPONENTIAL),
    enable_schedule=True,  # Enable adaptive scheduling
)
```

### **Advanced Configuration with Custom Translator**

```python
from dagster import RetryPolicy, AssetKey, Backoff
from dg_sqlmesh import sqlmesh_definitions_factory
from dg_sqlmesh import SQLMeshTranslator

class SlingToSqlmeshTranslator(SQLMeshTranslator):
    def get_external_asset_key(self, external_fqn: str) -> AssetKey:
        """
        Custom mapping for external assets.
        SQLMesh: 'jaffle_db.main.raw_source_customers' → Sling: ['target', 'main', 'raw_source_customers']
        """
        parts = external_fqn.replace('"', '').split('.')
        if len(parts) >= 3:
            catalog, schema, table = parts[0], parts[1], parts[2]
            return AssetKey(['target', 'main', table])
        return AssetKey(['external'] + parts[1:])

# All-in-one factory with custom translator (takes priority over external_asset_mapping)
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    gateway="postgres",
    translator=SlingToSqlmeshTranslator(),  # Custom translator takes priority
    external_asset_mapping="target/main/{node.name}",  # Ignored when translator is provided
    concurrency_limit=1,
    group_name="sqlmesh",
    op_tags={"team": "data", "env": "prod"},
    retry_policy=RetryPolicy(max_retries=1, delay=30.0, backoff=Backoff.EXPONENTIAL),
    enable_schedule=True,  # Enable adaptive scheduling
)
```

### **Advanced Configuration**

```python
from dagster import Definitions, RetryPolicy
from dg_sqlmesh import sqlmesh_assets_factory, sqlmesh_adaptive_schedule_factory
from dg_sqlmesh import SQLMeshResource
from dg_sqlmesh import SQLMeshTranslator

# SQLMesh resource configuration
sqlmesh_resource = SQLMeshResource(
    project_dir="sqlmesh_project",
    gateway="postgres",
    translator=SlingToSqlmeshTranslator(),
    concurrency_limit=1,
)

# SQLMesh assets configuration
sqlmesh_assets = sqlmesh_assets_factory(
    sqlmesh_resource=sqlmesh_resource,
    group_name="sqlmesh",
    op_tags={"team": "data", "env": "prod"},
    retry_policy=RetryPolicy(max_retries=1, delay=30.0, backoff=Backoff.EXPONENTIAL),
)

# Adaptive schedule and job created automatically
sqlmesh_adaptive_schedule, sqlmesh_job, _ = sqlmesh_adaptive_schedule_factory(
    sqlmesh_resource=sqlmesh_resource
)

defs = Definitions(
    assets=[sqlmesh_assets],
    jobs=[sqlmesh_job],
    schedules=[sqlmesh_adaptive_schedule],
    resources={
        "sqlmesh": sqlmesh_resource,
    },
)
```

## External Asset Mapping

### **🆕 NEW: Jinja2 Template Mapping**

The `external_asset_mapping` parameter allows you to easily map external SQLMesh sources (like Sling objects) to Dagster asset keys using Jinja2 templates:

```python
from dg_sqlmesh import sqlmesh_definitions_factory

# Simple mapping: external sources → target/main/{table_name}
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    external_asset_mapping="target/main/{node.name}",
    # ...
)

# Advanced mapping with database and schema
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    external_asset_mapping="{node.database}/{node.schema}/{node.name}",
    # ...
)

# Custom prefix mapping
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    external_asset_mapping="sling/{node.name}",
    # ...
)
```

### **Available Template Variables**

The following variables are available in your Jinja2 template:

- **`{node.database}`** : Database name (e.g., "jaffle_db")
- **`{node.schema}`** : Schema name (e.g., "main")
- **`{node.name}`** : Table name (e.g., "raw_source_customers")
- **`{node.fqn}`** : Full qualified name (e.g., "jaffle_db.main.raw_source_customers")

### **Examples**

```python
# Map to dbt-style naming
external_asset_mapping="target/main/{node.name}"
# Result: "jaffle_db.main.raw_source_customers" → ["target", "main", "raw_source_customers"]

# Map to database/schema/table structure
external_asset_mapping="{node.database}/{node.schema}/{node.name}"
# Result: "jaffle_db.main.raw_source_customers" → ["jaffle_db", "main", "raw_source_customers"]

# Map to custom prefix
external_asset_mapping="sling/{node.name}"
# Result: "jaffle_db.main.raw_source_customers" → ["sling", "raw_source_customers"]

# Map to simplified structure
external_asset_mapping="{node.name}"
# Result: "jaffle_db.main.raw_source_customers" → ["raw_source_customers"]
```

### **Conflict Resolution**

When both `translator` and `external_asset_mapping` are provided, the custom translator takes priority:

```python
# Custom translator takes priority
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    translator=MyCustomTranslator(),  # ✅ Used
    external_asset_mapping="target/main/{node.name}",  # ❌ Ignored
    # ...
)
```

A warning will be issued when both are provided to clarify the behavior.

## Custom Translator

To map external assets (SQLMesh sources) to your Dagster conventions, you can create a custom translator:

```python
from dg_sqlmesh import SQLMeshTranslator
import dagster as dg

class MyCustomTranslator(SQLMeshTranslator):
    def get_external_asset_key(self, external_fqn: str) -> dg.AssetKey:
        """
        Custom mapping for external assets.
        Example: 'jaffle_db.main.raw_source_customers' → ['target', 'main', 'raw_source_customers']
        """
        parts = external_fqn.replace('"', '').split('.')
        # We ignore the catalog (jaffle_db), we take the rest
        return dg.AssetKey(['target'] + parts[1:])

    def get_group_name(self, context, model) -> str:
        """
        Custom mapping for groups.
        """
        model_name = getattr(model, "view_name", "")
        if model_name.startswith("stg_"):
            return "staging"
        elif model_name.startswith("mart_"):
            return "marts"
        return super().get_group_name(context, model)
```

## Translator Methods

The `SQLMeshTranslator` exposes several methods you can override:

### `get_external_asset_key(external_fqn: str) -> AssetKey`

Maps an external asset FQN to a Dagster AssetKey.

### `get_asset_key(model) -> AssetKey`

Maps a SQLMesh model to a Dagster AssetKey.

### `get_group_name(context, model) -> str`

Determines the group for a model.

### `get_tags(context, model) -> dict`

Generates tags for a model.

### `get_metadata(model, keys: list[str]) -> dict`

Extracts specified metadata from the model.

## Asset Checks and Audits

### **Automatic Audit Conversion**

SQLMesh audits are automatically converted to Dagster AssetCheckSpec:

```python
# SQLMesh audit
MODEL (
    name customers,
    audits (
        not_null(column=id),
        unique_values(columns=[id, email])
    )
);

# Automatically becomes in Dagster
AssetCheckSpec(
    name="not_null",
    asset=AssetKey(["customers"]),
    blocking=False,  # SQLMesh handles blocking
    description="SQLMesh audit: not_null(column=id)"
)
```

### **AssetCheckResult Emission**

During execution, audit results are emitted as AssetCheckResult:

```python
AssetCheckResult(
    passed=True,
    asset_key=AssetKey(["customers"]),
    check_name="not_null",
    metadata={
        "sqlmesh_model_name": "customers",
        "audit_query": "SELECT COUNT(*) FROM customers WHERE id IS NULL",
        "audit_blocking": False,
        "audit_dialect": "postgres",
        "audit_args": {"column": "id"}
    }
)
```

## Adaptive Scheduling

### **Automatic Cron Analysis**

The system automatically analyzes all SQLMesh crons and determines the finest granularity:

```python
# If you have models with different crons:
# - customers: @daily
# - orders: @hourly
# - events: */5 * * * * (every 5 minutes)

# The adaptive schedule will be: */5 * * * * (every 5 minutes)
```

### **Intelligent Execution**

The schedule runs `sqlmesh run` on all models, but SQLMesh automatically manages which models should be executed:

```python
# The schedule simply does:
sqlmesh_resource.context.run(
    execution_time=datetime.datetime.now(),
)
```

## Architecture

### **Individual Asset Pattern**

Each SQLMesh model becomes a separate Dagster asset that:

- **Materializes independently** : Each asset calls `sqlmesh.materialize_assets_threaded()` for its specific model
- **Controls success/failure** : Each asset can succeed or fail individually based on SQLMesh execution results
- **Handles dependencies** : Uses `translator.get_model_deps_with_external()` for proper dependency mapping
- **Manages checks** : Each asset handles its own audit results with `AssetCheckResult` outputs

### **Benefits of Individual Assets**

- **Granular control** : Each asset can succeed or fail independently in the Dagster UI
- **Clear visibility** : See exactly which models are running, succeeded, or failed
- **Individual retries** : Failed assets can be retried without affecting others
- **Better monitoring** : Track performance and issues per model
- **Flexible scheduling** : Different assets can have different schedules if needed

### **SQLMeshResource**

- Manages SQLMesh context and caching
- Implements strict singleton pattern
- Uses AnyIO for multithreading
- Accepts a custom translator

### **SQLMeshTranslator**

- Maps SQLMesh concepts to Dagster
- Extensible via inheritance
- Handles external assets and dependencies

### **SQLMesh Metadata via Tags**

You can pass metadata from SQLMesh models to Dagster assets using the tag convention `dagster:property:value`:

```sql
-- In your SQLMesh model
MODEL (
    name customers,
    tags ARRAY["dagster:group_name:sqlmesh_datamarts"],
    -- ... other model properties
);
```

#### **Supported Properties**

Currently supported Dagster properties via tags:

- **`dagster:group_name:value`** : Sets the Dagster asset group name
  - Example: `"dagster:group_name:sqlmesh_datamarts"`
  - Result: Asset will be in the "sqlmesh_datamarts" group

#### **Tag Convention**

The convention follows the pattern: `dagster:property:value`

- **`dagster`** : Prefix to indicate this is for Dagster
- **`property`** : The Dagster property to update on the asset
- **`value`** : The value to set for that property

#### **Priority Order**

When determining asset properties, the translator follows this priority:

1. **SQLMesh tags** : `dagster:group_name:value` (highest priority)
2. **Factory parameter** : `group_name="sqlmesh"` in factory call
3. **Default logic** : Automatic group determination based on model path

### **sqlmesh_definitions_factory**

- All-in-one factory for simple configuration
- Automatically creates: resource, assets, job, schedule
- Validates external dependencies
- Returns Definitions directly

### **SQLMeshEventCaptureConsole**

- Custom SQLMesh console to capture events
- Captures audit results for AssetCheckResult
- Handles metadata serialization

## Plan + Run Architecture

### **Individual Asset Materialization**

Each Dagster asset materializes its specific SQLMesh model using:

1. **Model Selection** : `get_models_to_materialize()` selects the specific model for the asset
2. **Materialization** : `sqlmesh.materialize_assets_threaded()` executes the model
3. **Result Handling** : Console events determine success/failure and audit results

### **Implementation Details**

```python
# In each individual asset
def model_asset(context: AssetExecutionContext, sqlmesh: SQLMeshResource):
    # Materialize this specific model
    models_to_materialize = get_models_to_materialize(
        [current_asset_spec.key],
        sqlmesh.get_models,
        sqlmesh.translator,
    )

    # Execute materialization
    plan = sqlmesh.materialize_assets_threaded(models_to_materialize, context=context)

    # Check results via console events
    failed_models_events = sqlmesh._console.get_failed_models_events()
    evaluation_events = sqlmesh._console.get_evaluation_events()

    # Return MaterializeResult + AssetCheckResult for audits
    return MaterializeResult(...), *check_results
```

This approach provides granular control while maintaining all SQLMesh integration features.

## Performance

- **Shared execution**: A single SQLMesh run is triggered per Dagster run. The first selected asset starts the SQLMesh materialization for all selected models; subsequent assets reuse the captured results via `SQLMeshResultsResource` to determine what was materialized or skipped.
- **Strict singleton** : Only one active SQLMesh instance
- **Caching** : Contexts, models and translators are cached
- **Multithreading** : Uses AnyIO to avoid Dagster blocking
- **Lazy loading** : Resources are loaded on demand
- **Early validation** : External dependencies validation before execution
- **Optimized execution** : SQLMesh automatically skips models that don't need materialization

## Development Workflow

### **SQLMesh Development Philosophy**

This module follows SQLMesh's philosophy of **separation of concerns**:

- **Development** : Use SQLMesh CLI for development and schema changes
- **Production** : Use SQLMesh CLI for promoting changes
- **Orchestration** : Use this Dagster module only for running models

### **Development Workflow**

#### **1. Local Development**

```bash
# Develop your models locally
sqlmesh plan dev
sqlmesh apply dev

# Test your changes
sqlmesh run dev
```

#### **2. Production Promotion**

```bash
# Promote changes to production
sqlmesh plan prod # ->manual operation to validate the plan (apply it)

# Or use CI/CD pipeline
```

#### **3. Dagster Orchestration**

```python
# Dagster takes over for production runs
# - Automatic scheduling via adaptive schedule
# - Manual runs via Dagster UI
# - Only executes: sqlmesh run prod
```

### **Module Responsibilities**

#### **What this module DOES:**

- ✅ **Orchestrates** `sqlmesh run` commands
- ✅ **Schedules** model execution
- ✅ **Monitors** execution and audits
- ✅ **Emits** Dagster events and metadata

#### **What this module DOES NOT:**

- ❌ **Plan changes** (`sqlmesh plan`)
- ❌ **Apply changes** (`sqlmesh apply`)
- ❌ **Handle breaking changes**
- ❌ **Manage environments**

### **Breaking Changes Management**

Breaking changes are handled **outside** this module:

- **Development** : `sqlmesh plan dev` + manual review
- **Production** : `sqlmesh plan prod` + CI/CD approval
- **Orchestration** : This module only runs approved models

### **Environment Separation**

```bash
# Development (SQLMesh CLI)
sqlmesh plan dev
sqlmesh apply dev
sqlmesh run dev

# Production (Dagster module)
# Automatically runs: sqlmesh run prod
# Based on schedules and triggers
```

This separation ensures:

- ✅ **Clear responsibilities** : Development vs Orchestration
- ✅ **Safe deployments** : Breaking changes handled by SQLMesh CLI
- ✅ **Reliable orchestration** : Dagster only runs approved models
- ✅ **CI/CD friendly** : Standard SQLMesh workflow for deployments

### **Quick Testing Commands**

For quick testing of individual models during development:

```bash
# Test a single model (useful for debugging)
uv run dg launch --assets "jaffle_db/sqlmesh_jaffle_platform/stg_tweets"

# Test multiple models
uv run dg launch --assets "jaffle_db/sqlmesh_jaffle_platform/stg_customers,jaffle_db/sqlmesh_jaffle_platform/stg_orders"

# Test ALL SQLMesh models at once
uv run dg launch --assets "jaffle_db/sqlmesh_jaffle_platform/seed_model,jaffle_db/sqlmesh_jaffle_platform/stg_customers,jaffle_db/sqlmesh_jaffle_platform/stg_order_items,jaffle_db/sqlmesh_jaffle_platform/stg_orders,jaffle_db/sqlmesh_jaffle_platform/stg_products,jaffle_db/sqlmesh_jaffle_platform/stg_stores,jaffle_db/sqlmesh_jaffle_platform/stg_supplies,jaffle_db/sqlmesh_jaffle_platform/stg_tweets,jaffle_db/sqlmesh_jaffle_platform/incremental_model,jaffle_db/sqlmesh_jaffle_platform/products,jaffle_db/sqlmesh_jaffle_platform/stores,jaffle_db/sqlmesh_jaffle_platform/customers,jaffle_db/sqlmesh_jaffle_platform/order_items,jaffle_db/sqlmesh_jaffle_platform/full_model,jaffle_db/sqlmesh_jaffle_platform/supplies,jaffle_db/sqlmesh_jaffle_platform/tweets,jaffle_db/sqlmesh_jaffle_platform/orders"

# Test with specific partition
uv run dg launch --assets "jaffle_db/sqlmesh_jaffle_platform/stg_tweets" --partition "2024-01-01"
```

These commands are particularly useful for:

- ✅ **Testing skip behavior** : Models with `@daily` cron will be skipped outside their schedule
- ✅ **Debugging audit failures** : Quick feedback on blocking vs non-blocking audits
- ✅ **Verifying execution status** : See the new `sqlmesh_execution_status` AssetCheck results

## Dagster Component (YAML Configuration)

The module also provides a Dagster component for declarative YAML configuration:

### **Component Usage**

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
  # schedule_name and enable_schedule are optional with defaults
  # schedule_name: "sqlmesh_adaptive_schedule"  # default value
  # enable_schedule: true  # default value (creates schedule but doesn't activate it)
  external_asset_mapping: "target/main/{node.name}"
```

### **Scaffolding**

Create a new SQLMesh project with the component:

```bash
# Create a new SQLMesh project
dagster scaffold component dg_sqlmesh.SQLMeshProjectComponent --init

# Or scaffold with an existing project
dagster scaffold component dg_sqlmesh.SQLMeshProjectComponent --project-path path/to/your/sqlmesh_project
```

### **Component Features**

- **Declarative Configuration**: Configure SQLMesh integration through YAML
- **Automatic Asset Creation**: SQLMesh models become Dagster assets automatically
- **Audit Integration**: SQLMesh audits become Dagster asset checks
- **Adaptive Scheduling**: Automatic schedule creation based on SQLMesh crons
- **Scaffolding**: Generate new SQLMesh projects with `dagster scaffold`

For more details, see the [component documentation](examples/components/sqlmesh_project/README.md).

## Installation

```bash
pip install dg-sqlmesh
```

## Requirements

- Python 3.11+
- Dagster 1.11.4+
- SQLMesh 0.206.1+

## Required Dagster instance configuration (mandatory)

To guarantee safe, single-run orchestration of SQLMesh from Dagster, you must configure your Dagster instance to enforce a singleton concurrency on the module’s exposed key and to use a queued run coordinator.

1. Enforce queued run coordinator

```yaml
run_coordinator:
  module: dagster._core.run_coordinator.queued_run_coordinator
  class: QueuedRunCoordinator
```

2. Enforce singleton on the module’s concurrency key

Recommended (Dagster OSS 1.11+):

```yaml
tag_concurrency_limits:
  - key: dagster/concurrency_key
    value: sqlmesh_jobs_exclusive
    limit: 1
```

Alternative (if using instance-level concurrency config):

```yaml
concurrency:
  - concurrency_key: sqlmesh_jobs_exclusive
    limit: 1
```

Notes:

- The module tags jobs with `dagster/concurrency_key=sqlmesh_jobs_exclusive`. Do not change this key.
- Manual materializations will hard-fail with a non-retriable error if the instance is not using `QueuedRunCoordinator`.
- The adaptive schedule also validates the coordinator at tick time and will skip/stop runs if another run is already active.

### Why this is required (SQLMesh guidance)

SQLMesh does not encourage running multiple SQLMesh commands in parallel against the same project/environment. This module enforces a singleton execution to align with that guidance and avoid:

- State corruption or overwrites (e.g., snapshot/store inconsistencies)
- Conflicting DDL and schema migrations
- Race conditions on plan/apply/promote/invalidate operations
- Inconsistent audits and backfills
- Resource contention between concurrent jobs and any SQLMesh janitor/background tasks

## Limitations

- **Shared execution model** : A single `sqlmesh run` is triggered per Dagster run; assets reuse shared results via `SQLMeshResultsResource`
- **No Dagster → SQLMesh backfill** : Partitions managed only by SQLMesh itself (run a materialization to backfill)
- **Breaking changes** : Handled outside the module (SQLMesh CLI or CI/CD)
- **Environment management** : SQLMesh CLI or CI/CD
- **External asset mapping** : Only supports basic Jinja2 templates, complex conditionals may not work as expected
- **Schedule activation** : Schedules are created but not automatically activated (manual activation required)

## Troubleshooting

### Common Issues

#### **External asset mapping errors**

- **Cause** : Translator doesn't handle FQN format
- **Solution** : Check `get_external_asset_key` method

#### **External asset mapping template errors**

- **Cause** : Invalid Jinja2 template syntax or unsupported variables
- **Solution** : Use only supported variables: `{node.database}`, `{node.schema}`, `{node.name}`, `{node.fqn}`
- **Example** : `"target/main/{node.name}"` ✅ vs `"target/main/{{ node.name }}"` ❌

#### **Performance issues**

- **Cause** : Too many models loaded
- **Solution** : Use `concurrency_limit` and caching

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

Apache 2.0 License - see LICENSE file for details.
