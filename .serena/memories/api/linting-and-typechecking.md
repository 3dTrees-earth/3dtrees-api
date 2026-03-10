# Python Linting & Type Checking

For the 3dtrees-api repo, always run these checks after making changes:

- **Linting**: `uvx ruff check` (and `uvx ruff format --check` for formatting)
- **Type checking**: `uvx ty check`

Always use `uvx` to run these tools, never bare `ruff` or `ty`.
