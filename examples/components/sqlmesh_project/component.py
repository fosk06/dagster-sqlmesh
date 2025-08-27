from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any, Optional, Union

from dagster import Resolvable
from dagster._core.definitions.definitions_class import Definitions
from dagster._core.execution.context.asset_execution_context import AssetExecutionContext
from dagster._utils.cached_method import cached_method
from dagster.components.component.component import Component
from dagster.components.core.component_tree import ComponentTree
from dagster.components.core.context import ComponentLoadContext
from dagster.components.resolved.core_models import OpSpec, ResolutionContext
from dagster.components.resolved.model import Resolver
from dagster.components.scaffold.scaffold import scaffold_with
from dagster.components.utils.translation import TranslationFn, TranslationFnResolver

from dg_sqlmesh import sqlmesh_definitions_factory, SQLMeshResource, SQLMeshTranslator
from dg_sqlmesh.components.sqlmesh_project.scaffolder import SQLMeshProjectComponentScaffolder

@dataclass(frozen=True)
class SQLMeshComponentSettings:
    """Settings for SQLMesh component configuration."""

    enable_code_references: bool = True


@dataclass
class SQLMeshProjectArgs(Resolvable):
    """Aligns with SQLMesh project configuration."""

    project_dir: str
    gateway: str = "postgres"
    environment: str = "prod"
    concurrency_limit: int = 1


def resolve_sqlmesh_project(context: ResolutionContext, model) -> str:
    if isinstance(model, str):
        return context.resolve_source_relative_path(
            context.resolve_value(model, as_type=str),
        )

    args = SQLMeshProjectArgs.resolve_from_model(context, model)
    return context.resolve_source_relative_path(args.project_dir)


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
    testing, and deployment capabilities for SQL-based data pipelines.
    """

    project: Annotated[
        str,
        Resolver(
            resolve_sqlmesh_project,
            model_field_type=Union[str, SQLMeshProjectArgs.model()],
            description="The path to the SQLMesh project or a mapping defining a SQLMesh project",
            examples=[
                "{{ project_root }}/path/to/sqlmesh_project",
                {
                    "project_dir": "path/to/sqlmesh_project",
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
    translation: Annotated[
        Optional[TranslationFn[Mapping[str, Any]]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"model": data}),
    ] = None
    gateway: Annotated[
        str,
        Resolver.default(
            description="The SQLMesh gateway to use for execution.",
            examples=["postgres", "duckdb"],
        ),
    ] = "postgres"
    environment: Annotated[
        str,
        Resolver.default(
            description="The SQLMesh environment to use for execution.",
            examples=["dev", "prod"],
        ),
    ] = "prod"
    concurrency_limit: Annotated[
        int,
        Resolver.default(
            description="The concurrency limit for SQLMesh execution.",
        ),
    ] = 1
    name: Annotated[
        str,
        Resolver.default(
            description="The name for the SQLMesh assets.",
        ),
    ] = "sqlmesh_assets"
    group_name: Annotated[
        str,
        Resolver.default(
            description="The group name for the SQLMesh assets.",
        ),
    ] = "sqlmesh"
    op_tags: Annotated[
        Optional[Mapping[str, Any]],
        Resolver.default(
            description="Tags to apply to the SQLMesh assets.",
            examples=[{"team": "data", "env": "prod"}],
        ),
    ] = None
    schedule_name: Annotated[
        str,
        Resolver.default(
            description="The name for the adaptive schedule.",
        ),
    ] = "sqlmesh_adaptive_schedule"
    enable_schedule: Annotated[
        bool,
        Resolver.default(
            description="Whether to enable the adaptive schedule based on SQLMesh crons.",
        ),
    ] = False
    translation_settings: Annotated[
        Optional[SQLMeshComponentSettings],
        Resolver.default(
            description="Allows enabling or disabling various features for translating SQLMesh models into Dagster assets.",
            examples=[
                {
                    "enable_code_references": True,
                },
            ],
        ),
    ] = None

    @cached_property
    def translator(self):
        if self.translation:
            return ProxySQLMeshTranslator(self.translation)
        return SQLMeshTranslator()

    @cached_property
    def sqlmesh_resource(self):
        return SQLMeshResource(
            project_dir=self.project,
            gateway=self.gateway,
            environment=self.environment,
            concurrency_limit=self.concurrency_limit,
            translator=self.translator,
        )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # Create definitions using our factory
        defs = sqlmesh_definitions_factory(
            project_dir=self.project,
            gateway=self.gateway,
            environment=self.environment,
            concurrency_limit=self.concurrency_limit,
            translator=self.translator,
            name=self.name,
            group_name=self.group_name,
            op_tags=self.op_tags,
            schedule_name=self.schedule_name,
            enable_schedule=self.enable_schedule,
        )

        return defs

    def execute(self, context: AssetExecutionContext) -> Iterator:
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

    def __init__(self, fn: TranslationFn):
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


def get_projects_from_sqlmesh_component(components: Path) -> list[str]:
    """Get all SQLMesh projects from components."""
    project_components = ComponentTree.for_project(components).get_all_components(
        of_type=SQLMeshProjectComponent
    )

    return [component.project for component in project_components]
