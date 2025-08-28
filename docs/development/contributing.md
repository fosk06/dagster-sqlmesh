# Contributing to dg-sqlmesh

Thank you for your interest in contributing to dg-sqlmesh! This guide will help you get started.

## Development Setup

### 1. Clone the Repository

```bash
git clone https://github.com/fosk06/dagster-sqlmesh.git
cd dagster-sqlmesh
```

### 2. Install Dependencies

```bash
# Install development dependencies
uv sync --group dev

# Install in development mode
uv pip install -e .
```

### 3. Load Test Data

```bash
# Load test data for development
uv run --group dev python tests/load_jaffle_data.py
```

## Development Workflow

### 1. Create a Feature Branch

```bash
# Delete previous branches (if any)
git branch -D feat/your-feature-name

# Create new feature branch
git checkout -b feat/your-feature-name
```

### 2. Make Your Changes

- Write your code
- Add tests for new functionality
- Update documentation if needed
- Follow the code standards

### 3. Run Quality Checks

Before committing, run these checks:

```bash
# Code quality (MANDATORY)
uv run ruff check --ignore=F401,F811,E402 src/dg_sqlmesh/ tests/
uv run ruff format --check src/dg_sqlmesh/ tests/

# Dead code detection
uv run vulture src/dg_sqlmesh/ --min-confidence 80

# Run tests
uv run coverage run -m pytest tests/ -v
uv run coverage report --show-missing
```

### 4. Commit and Push

```bash
git add .
git commit -m "feat: descriptive message about your changes"
git push origin feat/your-feature-name
```

## Code Standards

### Python Code

- Follow PEP 8 style guidelines
- Use type hints where appropriate
- Write docstrings for public functions
- Keep functions focused and small

### Testing

- Write tests for new functionality
- Maintain test coverage above 60%
- Use descriptive test names
- Test both success and failure cases

### Documentation

- Update README.md for user-facing changes
- Add docstrings for new functions
- Update examples if APIs change
- Keep documentation in sync with code

## Testing

### Run Tests

```bash
# Run all tests
uv run pytest tests/ -v

# Run specific test file
uv run pytest tests/unit/test_factory.py -v

# Run with coverage
uv run coverage run -m pytest tests/ -v
uv run coverage report --show-missing
```

### Test Data

The project includes test data for development:

```bash
# Load test data
uv run --group dev python tests/load_jaffle_data.py

# Test SQLMesh integration
uv run --group dev sqlmesh -p tests/sqlmesh_project plan --no-prompts
```

## Pre-PR Checklist

Before creating a pull request, ensure:

- [ ] All tests pass (100% success rate)
- [ ] Code is properly linted with Ruff
- [ ] Code is properly formatted
- [ ] No dead code detected
- [ ] Package builds successfully
- [ ] Documentation updated (if applicable)
- [ ] GitHub Release notes will be updated (for notable changes)
- [ ] ADR created/updated (for architectural decisions)
- [ ] Examples updated (if public API changed)

## Creating a Pull Request

### 1. Run Local Validation

```bash
# Complete pre-PR validation
make validate  # Runs: clean build test ruff vulture
```

### 2. Create PR

```bash
gh pr create \
  --title "feat: your descriptive title" \
  --body "## Overview

Detailed description of changes...

## Testing
- ✅ All ruff checks pass
- ✅ All tests pass
- ✅ Package builds successfully
- ✅ No breaking changes

## Related
- Fixes #issue_number" \
  --draft
```

### 3. Mark Ready for Review

```bash
gh pr ready
```

## Release Process

### 1. Merge PR

After PR review and approval:

```bash
gh pr merge --rebase --delete-branch --admin
```

### 2. Create Release

```bash
# Create patch release
make release-patch

# Or create tag manually
git tag -a v1.9.2 -m "Release v1.9.2"
git push origin v1.9.2
```

## Architecture Decisions

The project follows documented architecture decisions in the `adr/` folder:

- **ADR-0001**: Individual assets vs multi-asset
- **ADR-0002**: Shared SQLMesh execution
- **ADR-0003**: Asset check integration
- **ADR-0004**: Retry policy management
- **ADR-0005**: SQLMesh-Dagster tag convention

## Getting Help

- **GitHub Issues**: [Create an issue](https://github.com/fosk06/dagster-sqlmesh/issues)
- **Discussions**: [GitHub Discussions](https://github.com/fosk06/dagster-sqlmesh/discussions)
- **Code Review**: Request review from maintainers

## Thank You

Thank you for contributing to dg-sqlmesh! Your contributions help make this project better for everyone. 🙏
