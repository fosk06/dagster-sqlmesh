from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any, Callable, Optional
import re
from dagster import Resolvable, AssetKey
from dagster._core.definitions.definitions_class import Definitions
from dagster._utils.cached_method import cached_method
from dagster import ComponentTree
from dagster.components import (
    Component,
    Resolver,
    scaffold_with,
)
from dagster.components.resolved.core_models import OpSpec

from dg_sqlmesh import sqlmesh_definitions_factory, SQLMeshResource, SQLMeshTranslator
from .scaffolder import SQLMeshProjectComponentScaffolder


@dataclass
class SQLMeshConfig(Resolvable):
    """SQLMesh configuration parameters."""

    project_path: str
    gateway: str = "postgres"
    environment: str = "prod"


@dataclass
class SQLMeshProjectArgs(Resolvable):
    """Aligns with SQLMesh project configuration."""

    project_dir: str
    gateway: str = "postgres"
    environment: str = "prod"
    concurrency_limit: int = 1


def resolve_sqlmesh_config(context, model) -> SQLMeshConfig:
    """Resolve SQLMesh configuration from YAML."""
    return SQLMeshConfig.resolve_from_model(context, model)


def resolve_sqlmesh_project(context, model) -> str:
    if isinstance(model, str):
        return str(
            context.resolve_source_relative_path(
                context.resolve_value(model, as_type=str),
            )
        )

    args = SQLMeshProjectArgs.resolve_from_model(context, model)
    return str(context.resolve_source_relative_path(args.project_dir))


@scaffold_with(SQLMeshProjectComponentScaffolder)
@dataclass
class SQLMeshProjectComponent(Component, Resolvable):
    """Expose a SQLMesh project to Dagster as a set of assets.

    This component assumes that you have already set up a SQLMesh project. The component
    will automatically create Dagster assets from your SQLMesh models with support for
    audits, metadata, and adaptive scheduling.

    Scaffold by running `dagster scaffold component dg_sqlmesh.SQLMeshProjectComponent --project-path path/to/your/existing/sqlmesh_project`
    in the Dagster project directory.

    ### What is SQLMesh?

    SQLMesh is a data transformation platform that provides incremental processing,
    testing, and deployment capabilities for SQL-based data pipelines. It offers:

    - **Incremental Processing**: Efficient incremental updates for large datasets
    - **Testing & Audits**: Built-in data quality checks and validation
    - **Environment Management**: Support for dev, staging, and production environments
    - **SQL-First**: Native SQL with powerful macros and templating
    - **Deployment**: Safe schema changes and breaking change management

    ### Key Features

    - **Individual Asset Control**: Each SQLMesh model becomes a separate Dagster asset
    - **Automatic Audits**: SQLMesh audits are converted to Dagster asset checks
    - **External Asset Mapping**: Map external sources (like Sling) to Dagster asset keys
    - **Adaptive Scheduling**: Automatic schedule creation based on SQLMesh crons
    - **Metadata Integration**: Complete SQLMesh metadata (tags, descriptions, etc.)
    - **Custom Translators**: Extensible translator system for custom mappings

    ### Example Usage

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

    ### External Asset Mapping

    Use the `external_asset_mapping` parameter to map external SQLMesh sources to Dagster asset keys:

    ```python
    # Map to dbt-style naming
    external_asset_mapping: "target/main/{node.name}"
    # Result: "jaffle_db.main.raw_source_customers" → ["target", "main", "raw_source_customers"]

    # Map to database/schema/table structure
    external_asset_mapping: "{node.database}/{node.schema}/{node.name}"
    # Result: "jaffle_db.main.raw_source_customers" → ["jaffle_db", "main", "raw_source_customers"]
    ```

    ### Available Template Variables

    - **`{node.database}`**: Database name (e.g., "jaffle_db")
    - **`{node.schema}`**: Schema name (e.g., "main")
    - **`{node.name}`**: Table name (e.g., "raw_source_customers")
    - **`{node.fqn}`**: Full qualified name (e.g., "jaffle_db.main.raw_source_customers")
    """

    sqlmesh_config: Annotated[
        SQLMeshConfig,
        Resolver(
            resolve_sqlmesh_config,
            model_field_type=SQLMeshConfig.model(),
            description="SQLMesh configuration including project path, gateway, and environment",
            examples=[
                {
                    "project_path": "{{ project_root }}/sqlmesh_project",
                    "gateway": "postgres",
                    "environment": "prod",
                },
            ],
        ),
    ]
    op: Annotated[
        Optional[OpSpec],
        Resolver.default(
            description="Op related arguments to set on the generated SQLMesh assets",
            examples=[
                {
                    "name": "some_op",
                    "tags": {"tag1": "value"},
                    "backfill_policy": {"type": "single_run"},
                },
            ],
        ),
    ] = None
    external_asset_mapping: Annotated[
        Optional[str],
        Resolver.default(
            description="Jinja2 template for mapping external SQLMesh sources (like Sling objects) to Dagster asset keys. Available variables: {node.database}, {node.schema}, {node.name}, {node.fqn}",
            examples=[
                "target/main/{node.name}",
                "{node.database}/{node.schema}/{node.name}",
                "sling/{node.name}",
                "{node.name}",
            ],
        ),
    ] = None
    gateway: Annotated[
        str,
        Resolver.default(
            description="The SQLMesh gateway to use for execution. Common options include postgres, duckdb, snowflake, bigquery, etc.",
            examples=["postgres", "duckdb", "snowflake", "bigquery"],
        ),
    ] = "postgres"
    environment: Annotated[
        str,
        Resolver.default(
            description="The SQLMesh environment to use for execution. Common environments include dev, staging, prod.",
            examples=["dev", "staging", "prod"],
        ),
    ] = "prod"
    concurrency_jobs_limit: Annotated[
        int,
        Resolver.default(
            description="The concurrency limit for SQLMesh jobs execution. Higher values allow more parallel model execution.",
            examples=[1, 2, 4, 8],
        ),
    ] = 1
    default_group_name: Annotated[
        str,
        Resolver.default(
            description="The default group name for the SQLMesh assets. This determines how assets are organized in the Dagster UI.",
            examples=["sqlmesh", "data", "analytics", "staging"],
        ),
    ] = "sqlmesh"
    op_tags: Annotated[
        Optional[Mapping[str, Any]],
        Resolver.default(
            description="Tags to apply to the SQLMesh assets. These tags help organize and filter assets in the Dagster UI.",
            examples=[
                {"team": "data", "env": "prod"},
                {"owner": "data-team", "priority": "high"},
            ],
        ),
    ] = None
    # Note: RetryPolicy is not model compliant in Dagster Components
    # retry_policy: Optional[RetryPolicy] = None
    schedule_name: Annotated[
        str,
        Resolver.default(
            description="The name for the adaptive schedule. This schedule will automatically run SQLMesh models based on their cron configurations.",
            examples=[
                "sqlmesh_adaptive_schedule",
                "data_pipeline_schedule",
                "analytics_schedule",
            ],
        ),
    ] = "sqlmesh_adaptive_schedule"
    enable_schedule: Annotated[
        bool,
        Resolver.default(
            description="Whether to create the schedule (does not automatically activate it)",
            examples=[True, False],
        ),
    ] = True

    @cached_property
    def translator(self):
        if self.external_asset_mapping:
            return JinjaSQLMeshTranslator(self.external_asset_mapping)
        return SQLMeshTranslator()

    @cached_property
    def sqlmesh_resource(self):
        return SQLMeshResource(
            project_dir=self.sqlmesh_config.project_path,
            gateway=self.sqlmesh_config.gateway,
            environment=self.sqlmesh_config.environment,
            concurrency_limit=self.concurrency_jobs_limit,
            translator=self.translator,
        )

    def build_defs(self, context=None) -> Definitions:
        # Create definitions using our factory
        # Note: retry_policy is not supported by sqlmesh_definitions_factory yet
        defs = sqlmesh_definitions_factory(
            project_dir=self.sqlmesh_config.project_path,
            gateway=self.sqlmesh_config.gateway,
            environment=self.sqlmesh_config.environment,
            concurrency_limit=self.concurrency_jobs_limit,
            translator=self.translator,
            group_name=self.default_group_name,
            op_tags=self.op_tags,
            schedule_name=self.schedule_name,
            enable_schedule=self.enable_schedule,
        )

        return defs

    def execute(self, context) -> Iterator:
        # This method is not used in our current architecture
        # as SQLMesh execution is handled by the individual assets
        pass

    @cached_method
    def asset_key_for_model(self, model_name: str):
        # This would need to be implemented based on SQLMesh model resolution
        # For now, we'll return a simple asset key
        return f"sqlmesh_{model_name}"


class ProxySQLMeshTranslator(SQLMeshTranslator):
    """Proxy translator that uses a custom translation function."""

    def __init__(self, fn: Callable[[AssetKey, Any], AssetKey]):
        self._fn = fn
        super().__init__()

    def get_asset_key(self, model):
        base_asset_key = super().get_asset_key(model)
        return self._fn(base_asset_key, model)

    def get_group_name(self, context, model):
        base_group_name = super().get_group_name(context, model)
        return self._fn(base_group_name, model)

    def get_tags(self, context, model):
        base_tags = super().get_tags(context, model)
        return self._fn(base_tags, model)

    def get_external_asset_key(self, external_fqn: str) -> AssetKey:
        # Parse the FQN to extract database, schema, name
        parts = re.findall(r'"([^"]+)"', external_fqn)
        if len(parts) == 3:
            database, schema, name = parts
        else:
            # Fallback for unquoted format
            parts = external_fqn.replace('"', "").split(".")
            if len(parts) >= 3:
                database, schema, name = parts[0], parts[1], parts[2]
            else:
                # If we can't parse it properly, use the original
                return super().get_external_asset_key(external_fqn)

        # Create node data for translation function
        node_data = {
            "database": database,
            "schema": schema,
            "name": name,
            "fqn": external_fqn,
        }

        # Call the translation function
        result = self._fn(node_data)

        # Convert the result to an AssetKey
        if isinstance(result, str):
            segments = [seg.strip() for seg in result.split("/") if seg.strip()]
            return AssetKey(segments)
        elif isinstance(result, AssetKey):
            return result
        else:
            return super().get_external_asset_key(external_fqn)


class JinjaSQLMeshTranslator(SQLMeshTranslator):
    """Translator that uses Jinja2 templates for external asset key mapping."""

    def __init__(self, external_asset_mapping_template: str):
        self.external_asset_mapping_template = external_asset_mapping_template
        super().__init__()

    def get_external_asset_key(self, external_fqn: str) -> AssetKey:
        """
        Generates an AssetKey for an external asset using Jinja2 template.
        Template variables available:
        - node.database: The database name
        - node.schema: The schema name
        - node.name: The table/view name
        - node.fqn: The full qualified name
        """
        import re
        from jinja2 import Template

        # Parse the FQN to extract database, schema, name
        # Handle both quoted and unquoted formats
        parts = re.findall(r'"([^"]+)"', external_fqn)
        if len(parts) == 3:
            database, schema, name = parts
        else:
            # Fallback for unquoted format
            parts = external_fqn.replace('"', "").split(".")
            if len(parts) >= 3:
                database, schema, name = parts[0], parts[1], parts[2]
            else:
                # If we can't parse it properly, use the original
                return super().get_external_asset_key(external_fqn)

        # Create template context
        context = {
            "node": {
                "database": database,
                "schema": schema,
                "name": name,
                "fqn": external_fqn,
            }
        }

        # Convert {node.name} format to {{ node.name }} for Jinja2
        template_str = self.external_asset_mapping_template.replace(
            "{node.", "{{ node."
        ).replace("}", "}}")

        # Render the template
        template = Template(template_str)
        result = template.render(**context)

        # Convert the result to an AssetKey
        # Split on '/' to create the asset key segments
        segments = [seg.strip() for seg in result.split("/") if seg.strip()]
        return AssetKey(segments)


def get_projects_from_sqlmesh_component(components: Path) -> list[str]:
    """Get all SQLMesh projects from components."""
    project_components = ComponentTree.for_project(components).get_all_components(
        of_type=SQLMeshProjectComponent
    )

    return [component.project for component in project_components]
