# Quick Start

Get up and running with dg-sqlmesh in minutes! This guide will walk you through creating your first SQLMesh-Dagster integration.

## Prerequisites

Make sure you have completed the [installation](installation.md) and have:

- ✅ dg-sqlmesh installed
- ✅ A SQLMesh project directory
- ✅ Python 3.11+ environment

## Step 1: Create a SQLMesh Project

First, let's create a simple SQLMesh project for testing:

```bash
# Create a new SQLMesh project
sqlmesh init quickstart_demo
cd quickstart_demo

# Initialize with DuckDB for local testing
sqlmesh init --template duckdb
```

## Step 2: Add a Simple Model

Create your first SQLMesh model:

```sql
-- models/hello_world.sql
MODEL (
    name hello_world,
    kind FULL,
    cron '@daily'
);

SELECT
    'Hello from SQLMesh!' as message,
    CURRENT_TIMESTAMP as timestamp;
```

## Step 3: Create Dagster Integration

Now create a Python file to integrate with Dagster:

```python
# quickstart.py
from dagster import Definitions
from dg_sqlmesh import sqlmesh_definitions_factory

# Create all SQLMesh assets, jobs, and schedules
defs = sqlmesh_definitions_factory(
    project_dir=".",  # Current directory (your SQLMesh project)
    gateway="duckdb",  # Use DuckDB for local development
    concurrency_limit=1,
    group_name="quickstart",
    enable_schedule=True,  # Enable adaptive scheduling
)

# Export definitions for Dagster
if __name__ == "__main__":
    from dagster import launch_run
    from dagster._core.instance import DagsterInstance

    # Launch a manual run
    instance = DagsterInstance.ephemeral()
    result = launch_run(defs, instance=instance)
    print(f"Run launched: {result.run_id}")
```

## Step 4: Test Your Integration

### Option A: Run with Dagster CLI

```bash
# Start Dagster UI
dagster dev

# Or run directly
dagster launch --file quickstart.py
```

### Option B: Run with Python

```bash
# Run the script directly
python quickstart.py
```

### Option C: Test with Dagster Launch

```bash
# Install dagster-launch if needed
pip install dagster-launch

# Launch a run
dagster launch --file quickstart.py
```

## Step 5: Verify Results

Check that everything worked:

```bash
# Check SQLMesh status
sqlmesh run --environment dev

# Check Dagster UI (if running)
# Open http://localhost:3000 in your browser
```

## Complete Example

Here's a complete working example with multiple models:

```python
# complete_example.py
from dagster import Definitions, RetryPolicy, Backoff
from dg_sqlmesh import sqlmesh_definitions_factory

# Create comprehensive SQLMesh integration
defs = sqlmesh_definitions_factory(
    project_dir=".",
    gateway="duckdb",
    external_asset_mapping="raw/{node.name}",  # Map external sources
    concurrency_limit=1,
    group_name="demo",
    op_tags={"team": "data", "env": "dev"},
    retry_policy=RetryPolicy(max_retries=1, delay=30.0, backoff=Backoff.EXPONENTIAL),
    enable_schedule=True,
)

# Export for Dagster
if __name__ == "__main__":
    from dagster import launch_run
    from dagster._core.instance import DagsterInstance

    instance = DagsterInstance.ephemeral()
    result = launch_run(defs, instance=instance)
    print(f"✅ Run completed: {result.run_id}")
```

## SQLMesh Models for Testing

Create these additional models to test more features:

```sql
-- models/staging/customers.sql
MODEL (
    name stg_customers,
    kind INCREMENTAL,
    cron '@hourly'
);

SELECT
    id,
    name,
    email,
    created_at
FROM raw.customers
WHERE created_at > @start_date;

-- models/marts/customer_summary.sql
MODEL (
    name customer_summary,
    kind FULL,
    cron '@daily',
    audits (
        not_null(column=id),
        unique_values(columns=[id])
    )
);

SELECT
    id,
    name,
    email,
    COUNT(*) as order_count,
    SUM(total) as total_spent
FROM stg_customers c
JOIN stg_orders o ON c.id = o.customer_id
GROUP BY 1, 2, 3;
```

## What You've Built

Congratulations! You've created:

✅ **SQLMesh Models** - Data transformation logic  
✅ **Dagster Assets** - Orchestration and monitoring  
✅ **Automatic Scheduling** - Based on SQLMesh cron patterns  
✅ **Audit Integration** - SQLMesh audits as Dagster checks  
✅ **External Asset Mapping** - Integration with external sources

## Next Steps

Now that you have a basic integration working:

1. **[Configuration Guide](configuration.md)** - Learn about all configuration options
2. **[User Guide](../user-guide/core-concepts.md)** - Understand core concepts
3. **[Examples](../examples/basic-usage.md)** - See more complex use cases
4. **[API Reference](../api/factory-functions.md)** - Explore all available functions

## Troubleshooting

### Common Issues

#### SQLMesh Project Not Found

```bash
# Make sure you're in the right directory
pwd
ls -la  # Should show sqlmesh_project.yaml

# Check SQLMesh project structure
sqlmesh project info
```

#### Dagster Import Errors

```bash
# Verify Dagster installation
pip list | grep dagster

# Check Python path
python -c "import dagster; print(dagster.__version__)"
```

#### DuckDB Connection Issues

```bash
# For DuckDB, make sure the project directory is writable
chmod 755 .

# Check SQLMesh configuration
cat sqlmesh_project.yaml
```

---

<div class="admonition success" markdown>

**Quick Start Complete!** 🚀

You now have a working SQLMesh-Dagster integration. The next step is to explore the [configuration options](configuration.md) and build more complex data pipelines.

</div>
