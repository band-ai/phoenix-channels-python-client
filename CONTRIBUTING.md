# Contributing to Phoenix Channels Python Client

Thank you for your interest in contributing to Phoenix Channels Python Client! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager (recommended) or pip

### Initial Setup

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/phoenix-channels-python-client-alpha.git
   cd phoenix-channels-python-client-alpha
   ```
3. Add upstream remote:
   ```bash
   git remote add upstream https://github.com/thenvoi/phoenix-channels-python-client-alpha.git
   ```
4. Install dependencies:
   ```bash
   # Using uv (recommended)
   uv sync --all-extras

   # Or using pip
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -e ".[dev]"
   ```
5. Set up pre-commit hooks:
   ```bash
   pre-commit install
   pre-commit install --hook-type commit-msg
   ```

## Development Workflow

1. Create a feature branch from `main`:
   ```bash
   git checkout main
   git pull upstream main
   git checkout -b feat/your-feature-name
   # or fix/your-bug-fix for bug fixes
   ```

2. Make your changes following the code standards below

3. Run tests:
   ```bash
   pytest
   ```

4. Run pre-commit checks:
   ```bash
   pre-commit run --all-files
   ```

5. Commit your changes using [Conventional Commits](https://www.conventionalcommits.org/):
   ```bash
   git commit -m "feat: add new feature description"
   # or
   git commit -m "fix: resolve issue description"
   ```

6. Push and create a pull request to `main`

## Code Standards

### Style Guidelines

- **Formatter/Linter**: Ruff (88-character line limit)
- **Type Checker**: Pyrefly
- **Secret Detection**: Gitleaks

All formatting is enforced via pre-commit hooks.

### Type Annotations

- Use type hints for all function parameters and return types
- Use modern Python syntax where possible:
  ```python
  # Good
  def process(items: list[str]) -> dict[str, int]:
      ...

  def get_value(key: str) -> str | None:
      ...
  ```

### Async Code

This library is async-first. Follow these guidelines:
- Use `async def` for functions that perform I/O operations
- Use `await` for calling async functions
- Use `asyncio` for concurrency patterns

```python
async def handle_message(message):
    """Process incoming Phoenix Channel message."""
    # Your async handling logic here
    pass
```

## Testing

### Running Tests

```bash
# All tests
pytest

# With verbose output
pytest -v

# Specific test file
pytest tests/test_client.py

# Specific test
pytest -k "test_name"
```

### Writing Tests

- Place tests in the `tests/` directory
- Use pytest and pytest-asyncio for async tests
- Follow existing test patterns in the codebase

## Pull Request Guidelines

1. Ensure all tests pass
2. Ensure pre-commit checks pass
3. Update documentation if needed
4. Fill out the PR template completely
5. Request review from maintainers
6. Address any feedback

## Naming Conventions

### Issue Titles

Use component prefixes to categorize issues:

```
[Component] Brief description
```

**Components:**
- `[Client]` - PHXChannelsClient core functionality
- `[Protocol]` - Phoenix protocol handling (v1/v2)
- `[WebSocket]` - WebSocket connection management
- `[Events]` - Event handlers and message routing
- `[Docs]` - Documentation
- `[CI]` - CI/CD and workflows
- `[Performance]` - Performance improvements

**Examples:**
- `[Client] Add automatic reconnection support`
- `[Protocol] Fix v2 message parsing`
- `[WebSocket] Handle connection timeouts gracefully`

### PR Titles

Follow Conventional Commits format:

```
type(scope): description
```

**Types:** `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

**Examples:**
- `feat(client): add heartbeat configuration`
- `fix(protocol): resolve v2 join response parsing`
- `docs: update README with protocol examples`

PR titles are validated by CI - PRs with invalid titles will fail the check.

## Branch Naming

- `feat/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation changes

## Commit Messages

This project uses [Conventional Commits](https://www.conventionalcommits.org/) enforced by Commitizen:

- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `style:` - Code style changes (formatting, etc.)
- `refactor:` - Code refactoring
- `test:` - Adding or updating tests
- `chore:` - Maintenance tasks

## Release Process

Releases follow [Semantic Versioning](https://semver.org/):
- **MAJOR**: Breaking API changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

Releases are managed automatically via Release Please.

## Questions?

If you have questions or need help, please open an issue on GitHub.
