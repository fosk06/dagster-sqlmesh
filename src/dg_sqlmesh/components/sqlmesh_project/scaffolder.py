import os
from pathlib import Path
from typing import Optional

from dagster._core.errors import DagsterInvalidInvocationError
from dagster.components.component.component_scaffolder import Scaffolder
from dagster.components.component_scaffolding import scaffold_component
from dagster.components.scaffold.scaffold import ScaffoldRequest
from pydantic import BaseModel, Field

# SQLMesh imports for scaffolding
try:
    from sqlmesh import Context
except ImportError:
    Context = None


class SQLMeshScaffoldParams(BaseModel):
    init: bool = Field(default=False)
    project_path: Optional[str] = None


class SQLMeshProjectComponentScaffolder(Scaffolder[SQLMeshScaffoldParams]):
    @classmethod
    def get_scaffold_params(_cls) -> type[SQLMeshScaffoldParams]:
        return SQLMeshScaffoldParams

    def scaffold(self, request: ScaffoldRequest[SQLMeshScaffoldParams]) -> None:
        project_root = request.project_root or os.getcwd()

        if request.params.project_path:
            project_root_tmpl = "{{ project_root }}"
            rel_path = os.path.relpath(request.params.project_path, start=project_root)
            path_str = f"{project_root_tmpl}/{rel_path}"

        elif request.params.init:
            if Context is None:
                raise DagsterInvalidInvocationError(
                    "SQLMesh is not installed. Please install SQLMesh to scaffold this component."
                )

            # Create a basic SQLMesh project structure
            project_name = "sqlmesh_project"
            project_path = Path(project_root) / project_name

            if project_path.exists():
                raise DagsterInvalidInvocationError(
                    f"SQLMesh project directory {project_path} already exists."
                )

            # Create basic SQLMesh project structure
            project_path.mkdir()

            # Create config.yaml
            config_content = """# SQLMesh configuration
default_target: duckdb
default_sql_dialect: duckdb

model_defaults:
  dialect: duckdb
  materialized: table

targets:
  duckdb:
    type: duckdb
    path: ":memory:"
"""
            (project_path / "config.yaml").write_text(config_content)

            # Create models directory
            models_dir = project_path / "models"
            models_dir.mkdir()

            # Create a sample model
            sample_model_content = """MODEL (
  name sample_model,
  kind FULL,
  grain id,
  audits(
    number_of_rows(threshold := 1),
    not_null(columns := (id, name))
  )
);

SELECT 
  1 as id,
  'sample' as name
"""
            (models_dir / "sample_model.sql").write_text(sample_model_content)

            # Create seeds directory
            seeds_dir = project_path / "seeds"
            seeds_dir.mkdir()

            # Create a sample seed
            sample_seed_content = """id,name
1,Alice
2,Bob
3,Charlie
"""
            (models_dir / "seeds" / "sample_seed.csv").write_text(sample_seed_content)

            path_str = project_name
        else:
            path_str = None

        scaffold_component(request, {"project": path_str})
