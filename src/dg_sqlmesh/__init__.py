"""
Dagster SQLMesh Integration

A Python package that provides seamless integration between Dagster and SQLMesh
for modern data engineering workflows.
"""

__version__ = "1.9.3"
__author__ = "Thomas Trividic"

# Import main components for easy access
from .factory import (
    sqlmesh_definitions_factory,
    sqlmesh_assets_factory,
    sqlmesh_adaptive_schedule_factory,
)
from .resource import SQLMeshResource
from .translator import SQLMeshTranslator

# Import component for YAML configuration
try:
    from .components.sqlmesh_project import (
        SQLMeshProjectComponent,
        SQLMeshProjectComponentScaffolder,
    )

    COMPONENT_AVAILABLE = True
except ImportError:
    COMPONENT_AVAILABLE = False

__all__ = [
    "__version__",
    "__author__",
    "sqlmesh_definitions_factory",
    "sqlmesh_assets_factory",
    "sqlmesh_adaptive_schedule_factory",
    "SQLMeshResource",
    "SQLMeshTranslator",
]

# Add component exports if available
if COMPONENT_AVAILABLE:
    __all__.extend(
        [
            "SQLMeshProjectComponent",
            "SQLMeshProjectComponentScaffolder",
        ]
    )
    # Make imports available at module level
    locals()["SQLMeshProjectComponent"] = SQLMeshProjectComponent
    locals()["SQLMeshProjectComponentScaffolder"] = SQLMeshProjectComponentScaffolder
