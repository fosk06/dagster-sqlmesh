# Installation

This guide will help you install and set up dg-sqlmesh in your environment.

## Prerequisites

Before installing dg-sqlmesh, ensure you have:

- **Python 3.11+** installed
- **Dagster 1.11.4+** installed
- **SQLMesh 0.206.1+** installed
- Access to your **SQLMesh project directory**

## Installation Options

### Option 1: PyPI (Recommended)

Install the latest stable version from PyPI:

```bash
pip install dg-sqlmesh
```

### Option 2: Specific Version

Install a specific version:

```bash
pip install dg-sqlmesh==1.9.1
```

### Option 3: Development Version

Install directly from GitHub for the latest development features:

```bash
pip install git+https://github.com/fosk06/dagster-sqlmesh.git
```

### Option 4: With uv (Recommended for Development)

If you're using `uv` for dependency management:

```bash
# Add to your project
uv add dg-sqlmesh

# Or install globally
uv pip install dg-sqlmesh
```

## Package Dependencies

When you install dg-sqlmesh, the following dependencies are automatically installed:

### Core Dependencies

- **dagster** >= 1.11.4 - Core orchestration framework
- **sqlmesh[web]** >= 0.206.1 - Data modeling and transformation
- **pandas** >= 2.2.1 - Data manipulation and analysis

### Optional Dependencies

- **jinja2** >= 3.0.0 - Template engine for external asset mapping
- **duckdb** - For local development and testing

## Development Installation

For contributing to dg-sqlmesh:

```bash
# Clone the repository
git clone https://github.com/fosk06/dagster-sqlmesh.git
cd dagster-sqlmesh

# Install in development mode
pip install -e .

# Or with uv
uv pip install -e .
```

## Environment Setup

### 1. SQLMesh Project

Ensure you have a SQLMesh project configured:

```bash
# Create a new SQLMesh project (if needed)
sqlmesh init my_project

# Or use an existing project
cd path/to/your/sqlmesh_project
```

### 2. Dagster Instance

Configure your Dagster instance with the required settings:

```yaml
# dagster.yaml
run_coordinator:
  module: dagster._core.run_coordinator.queued_run_coordinator
  class: QueuedRunCoordinator

tag_concurrency_limits:
  - key: dagster/concurrency_key
    value: sqlmesh_jobs_exclusive
    limit: 1
```

### 3. Environment Variables

Set up any required environment variables:

```bash
# For PyPI publishing (if contributing)
export UV_PUBLISH_TOKEN=your_pypi_token

# For database connections (configure in SQLMesh)
export DATABASE_URL=your_database_url
```

## Verification

Test your installation:

```python
# Python
python -c "import dg_sqlmesh; print(dg_sqlmesh.__version__)"

# Or start Python and import
python
>>> import dg_sqlmesh
>>> print(dg_sqlmesh.__version__)
'1.9.1'
```

## Quick Test

Create a simple test to verify everything works:

```python
from dg_sqlmesh import sqlmesh_definitions_factory

# This should work without errors
defs = sqlmesh_definitions_factory(
    project_dir="path/to/your/sqlmesh_project",
    gateway="duckdb",  # Use DuckDB for testing
    concurrency_limit=1,
)
```

## Troubleshooting

### Common Installation Issues

#### Import Errors

```bash
# If you get import errors, check Python version
python --version  # Should be 3.11+

# Verify installation
pip list | grep dg-sqlmesh
```

#### Dependency Conflicts

```bash
# Create a fresh virtual environment
python -m venv dg_sqlmesh_env
source dg_sqlmesh_env/bin/activate  # On Windows: dg_sqlmesh_env\Scripts\activate

# Install fresh
pip install dg-sqlmesh
```

#### SQLMesh Not Found

```bash
# Install SQLMesh separately if needed
pip install sqlmesh[web]

# Verify SQLMesh installation
sqlmesh --version
```

### Getting Help

If you encounter issues:

1. **Check the [troubleshooting guide](../troubleshooting/common-issues.md)**
2. **Search [GitHub Issues](https://github.com/fosk06/dagster-sqlmesh/issues)**
3. **Create a new issue** with detailed error information

## Next Steps

Once installation is complete:

1. **[Quick Start](quick-start.md)** - Build your first integration
2. **[Configuration](configuration.md)** - Learn about all options
3. **[Examples](../examples/basic-usage.md)** - See practical usage examples

---

<div class="admonition success" markdown>

**Installation Complete!** 🎉

You're now ready to start using dg-sqlmesh. Head over to the [Quick Start guide](quick-start.md) to create your first SQLMesh-Dagster integration.

</div>
