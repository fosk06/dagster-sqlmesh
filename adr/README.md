# Architecture Decision Records (ADR)

This directory contains Architecture Decision Records (ADRs) for the `dg-sqlmesh` project. ADRs document significant architectural decisions, their context, rationale, and consequences.

## What are ADRs?

Architecture Decision Records are documents that capture important architectural decisions made during the development of a project. They provide:

- **Context**: Why the decision was needed
- **Decision**: What was decided
- **Rationale**: Why this option was chosen over alternatives
- **Consequences**: Both positive and negative outcomes

## ADR Index

| ADR                                                                       | Title                                                   | Status   | Date       |
| ------------------------------------------------------------------------- | ------------------------------------------------------- | -------- | ---------- |
| [ADR-0001](./0001-individual-assets-vs-multi-asset.md)                    | Individual Assets vs Multi-Asset Pattern                | Accepted | 2025-08-05 |
| [ADR-0002](./0002-shared-sqlmesh-execution.md)                            | Shared SQLMesh Execution per Dagster Run                | Accepted | 2025-08-05 |
| [ADR-0003](./0003-asset-check-integration.md)                             | Asset Check Integration for SQLMesh Audits              | Accepted | 2025-08-05 |
| [ADR-0004](./0004-retry-policy-management.md)                             | Retry Policy Management for SQLMesh Integration         | Accepted | 2025-08-05 |
| [ADR-0005](./0005-custom-sqlmesh-console.md)                              | Custom SQLMesh Console for Event Capture                | Accepted | 2025-08-05 |
| [ADR-0006](./0006-sqlmesh-dagster-tag-convention.md)                      | SQLMesh to Dagster Tag Convention                       | Accepted | 2025-08-05 |
| [ADR-0007](./0007-code-version-data-version-mapping.md)                   | Code Version and Data Version Mapping                   | Accepted | 2025-08-05 |
| [ADR-0008](./0008-sqlmesh-plan-run-flow.md)                               | SQLMesh Plan/Run Flow and Separation of Concerns        | Accepted | 2025-08-05 |
| [ADR-0009](./0009-shared-state-management-pattern.md)                     | Shared State Management Pattern                         | Accepted | 2025-01-27 |
| [ADR-0010](./0010-function-extraction-and-modularity.md)                  | Function Extraction and Modularity                      | Accepted | 2025-01-27 |
| [ADR-0011](./0011-error-handling-strategy.md)                             | Error Handling Strategy                                 | Accepted | 2025-01-27 |
| [ADR-0012](./0012-sqlmesh-dagster-component.md)                           | SQLMesh Dagster Component                               | Accepted | 2025-01-27 |
| [ADR-0013](./0013-sqlmesh-notifier-integration.md)                        | SQLMesh Notifier Integration                            | Accepted | 2025-01-27 |
| [ADR-0014](./0014-refactoring-plan-consolidation.md)                      | Refactoring Plan Consolidation                          | Accepted | 2025-01-27 |
| [ADR-0015](./0015-custom-sqlmesh-console-execution-tracking.md)           | Custom SQLMesh Console for Model Execution Tracking     | Accepted | 2025-08-27 |
| [ADR-0016](./0016-sqlmesh-model-skip-detection-assetcheck-integration.md) | SQLMesh Model Skip Detection and AssetCheck Integration | Accepted | 2025-08-27 |
| [ADR-0017](./0017-enhanced-context-composition-pattern.md) | EnhancedContext Composition Pattern with Metaprogramming | Accepted | 2025-09-04 |

## Key Architectural Patterns

### 1. Individual Asset Pattern

Each SQLMesh model becomes a separate Dagster asset, enabling granular control and better UI integration.

### 2. Shared Execution Pattern

Single SQLMesh execution per Dagster run, shared between all selected assets to respect SQLMesh's natural dependency management.

### 3. Event-Driven Architecture

Custom SQLMesh console captures execution events and converts them to Dagster concepts (asset checks, error messages, etc.).

### 4. Resource-Based State Management

`SQLMeshResultsResource` manages shared state between assets in the same run, ensuring consistency.

### 5. Tag-Based Property Mapping

`dagster:property_name:value` convention allows SQLMesh models to override Dagster asset properties via tags.

### 6. Version Mapping Strategy

SQLMesh `data_hash` maps to Dagster `code_version`, SQLMesh snapshot version maps to Dagster `data_version` for accurate sync status.

### 7. Plan/Run Flow and Separation of Concerns

Use SQLMesh `plan` for metadata extraction and `run` for materialization. Our module is a materialization orchestrator, not a workflow manager - we don't handle environments, breaking changes, or plan validation.

### 8. Shared State Management Pattern

`SQLMeshResultsResource` manages shared state between assets in the same run, ensuring single SQLMesh execution per run while maintaining proper isolation between different runs.

### 9. Function Extraction and Modularity

Large functions are broken down into smaller, focused utility functions to improve maintainability, testability, and readability while maintaining external API compatibility.

### 10. Layered Error Handling Strategy

Custom exception classes and error classification system that distinguishes between different error types and handles them appropriately without breaking the entire pipeline.

### 11. Declarative Component Configuration

SQLMesh Dagster Component provides declarative YAML configuration for SQLMesh projects, following dagster-dbt patterns with Jinja2 templating for external asset mapping and smart default values for improved user experience.

### 12. Custom Console Injection Pattern

Custom SQLMesh console (`SimpleRunTracker`) injected into `sqlmesh.context.console` to capture real-time execution events without modifying SQLMesh's internal API. This enables precise tracking of which models are actually executed vs skipped.

### 13. Execution Status AssetCheck Pattern

Automatic `sqlmesh_execution_status` AssetCheck added to all SQLMesh models, providing clear feedback to users about whether a model was executed or skipped by SQLMesh. This integrates seamlessly with Dagster's existing AssetCheck system.

### 14. EnhancedContext Composition Pattern

`EnhancedContext` uses composition with metaprogramming (`__getattr__`) to extend SQLMesh `Context` functionality while maintaining full API compatibility. This pattern provides dry-run capabilities, automatic method delegation, and robustness against SQLMesh API changes.

## Current Limitations

### Non-Blocking Audits

Non-blocking audits are supported. Failures are surfaced as Dagster checks with WARN severity, while SQLMesh manages the blocking semantics and downstream propagation for blocking audits.

### Schedule Complexity

Adaptive schedules based on SQLMesh crons require complex logic to determine optimal execution frequency.

### Error Type Distinction

Distinguishing between transient and persistent failures for retry logic is complex and not fully implemented.

## Future Enhancements

1. **Enhanced error classification** - Better distinction between transient and persistent failures
2. **Advanced scheduling** - More sophisticated adaptive scheduling based on data dependencies
3. **Performance optimization** - Reduce memory usage and improve execution speed

## Contributing

When making significant architectural changes:

1. **Create a new ADR** following the template
2. **Update existing ADRs** if decisions change
3. **Link related ADRs** to show dependencies
4. **Include diagrams** using Mermaid for complex flows
5. **Document consequences** both positive and negative

## Template

```markdown
# ADR-XXXX: [Title]

## Status

**Proposed/Accepted/Rejected** - YYYY-MM-DD

## Context

[Describe the context and problem statement]

## Decision

[Describe the decision made]

## Rationale

[Explain why this decision was made over alternatives]

## Implementation

[Describe how the decision is implemented]

## Consequences

### Positive

- [List positive consequences]

### Negative

- [List negative consequences]

## Related Decisions

- [Links to related ADRs]
```
