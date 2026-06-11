# mssqlclient-ng: AI Engineering Guide

This file is the canonical AI guidance for this repository.

## Runtime Requirement

- Python version baseline is 3.11+ (see [pyproject.toml](pyproject.toml): `requires-python = ">=3.11,<4.0"`).
- Do not introduce language features or dependencies incompatible with Python 3.11.
- Use `uv` for environment and dependency workflows in this repo.

## Read Order

1. [README.md](README.md) - CLI behavior, authentication modes, linked-server usage.
2. [src/mssqlclient_ng/cli.py](src/mssqlclient_ng/cli.py) - runtime flow and top-level command handling.
3. [tests](tests) - expected behavior and regression boundaries.

## Core Rule: [MSSQLand](https://github.com/n3rada/MSSQLand) Parity

mssqlclient-ng is the Python counterpart of MSSQLand. For action semantics, behavior should track [MSSQLand](https://raw.githubusercontent.com/n3rada/MSSQLand/refs/heads/main/AI.md) whenever practical.

- Prefer behavioral parity over inventing new UX or semantics.
- Keep action names, intent, and output conventions aligned with MSSQLand where possible.
- If behavior must differ because of Python/Impacket constraints, document why in code comments or docstrings and keep the difference minimal.

## Core Rule: Understand Impacket Internals

When a feature, bug, or edge case touches TDS, auth, or low-level protocol flow, inspect [impacket](https://github.com/fortra/impacket) internals before implementing workarounds.

- Review the relevant Impacket classes and call paths first. Do not hesitate to go take a look inside current virtual env.
- Prefer fixing usage of existing Impacket primitives over re-implementing protocol logic.
- Only add local abstraction when it simplifies correctness, testing, and maintainability.

## Python Rules

These are the mandatory Python development rules for this repo. 

1. Imports
- Prefer module imports for utility modules over importing individual utility functions.
- Keep imports at module top unless a local import is required to break a cycle or avoid optional import cost.
- Preserve clear import grouping: standard library, third-party, local.
- Add explicit section comments for each group: `# Built-in imports`, `# Third-party imports`, `# Local imports`.

2. Typing and modern Python
- Use modern Python 3.11 typing syntax such as `X | Y` and `X | None`.
- Prefer concrete types over `Any` when practical.
- Keep return types honest and narrow them explicitly when needed.

3. Error handling and logging
- Avoid broad `except Exception` unless re-raising, translating, or intentionally degrading behavior.
- Inside exception handlers, prefer `logger.exception(...)` if available from `loguru` when traceback context matters.

4. Code hygiene
- Keep comments concise and non-obvious.
- Prefer minimal diffs over broad refactors.
- Do not introduce repo-wide style churn while solving local problems.

## Source Map (Start Here)

- CLI entry point: [src/mssqlclient_ng/cli.py](src/mssqlclient_ng/cli.py)
- Interactive shell: [src/mssqlclient_ng/core/terminal.py](src/mssqlclient_ng/core/terminal.py)
- Action base and arg model: [src/mssqlclient_ng/core/actions/base.py](src/mssqlclient_ng/core/actions/base.py)
- Action registration: [src/mssqlclient_ng/core/actions/factory.py](src/mssqlclient_ng/core/actions/factory.py)
- DatabaseContext facade: [src/mssqlclient_ng/core/services/database.py](src/mssqlclient_ng/core/services/database.py)
- Query execution and wrapping: [src/mssqlclient_ng/core/services/query.py](src/mssqlclient_ng/core/services/query.py)
- Auth service: [src/mssqlclient_ng/core/services/authentication.py](src/mssqlclient_ng/core/services/authentication.py)
- Linked server model: [src/mssqlclient_ng/core/models/linked_servers.py](src/mssqlclient_ng/core/models/linked_servers.py)
- Formatting pipeline: [src/mssqlclient_ng/core/utils/formatters](src/mssqlclient_ng/core/utils/formatters)

## Design Principles (Strict)

1. SRP
- Keep each action focused on one capability.
- Keep chain wrapping and execution strategy in query services, not in action classes.

2. OOP with composition
- Actions depend on DatabaseContext services, not concrete internals spread across modules.
- Prefer composition in services over inheritance-heavy designs.

3. Open for extension
- Add actions via `@ActionFactory.register(...)` in new modules.
- Do not break existing action contracts when extending behavior.

4. DRY and consistency
- Reuse argument parsing and formatter helpers.
- Do not duplicate SQL wrapping or linked-server handling in actions.

5. Keep MSSQLand parity intentional
- Before changing action semantics, compare the equivalent MSSQLand behavior.
- Differences are acceptable only when Python runtime constraints, Impacket behavior, or transport details require them.

6. Fail fast and observable
- Surface errors with clear logs.
- Keep operational semantics predictable (timeouts, retries, chain behavior).

## Action Authoring Rules

When adding or changing an action:

1. Create module under the correct category in [src/mssqlclient_ng/core/actions](src/mssqlclient_ng/core/actions).
2. Register with `@ActionFactory.register(...)`.
3. Keep argument declaration and validation consistent with [src/mssqlclient_ng/core/actions/base.py](src/mssqlclient_ng/core/actions/base.py).
4. Ensure category `__init__.py` imports the new module so registration executes.
5. Route SQL execution through [src/mssqlclient_ng/core/services/query.py](src/mssqlclient_ng/core/services/query.py), not ad-hoc wrappers.
6. For ConfigMgr actions, follow [src/mssqlclient_ng/core/actions/configmgr/cm_base.py](src/mssqlclient_ng/core/actions/configmgr/cm_base.py).

## Git Conventions

- Commit messages must follow [Conventional Commits](https://www.conventionalcommits.org/): `type(scope): short description`. One-liner, no body unless strictly necessary.
- Always sign the commits with `-S`.

## Testing and Quality Gates

- Run tests with `uv run pytest -v`.
- Keep output stable for markdown/csv formatters.
- Preserve backwards-compatible CLI behavior unless a breaking change is explicitly intended.

## Definition of Done

A change is complete only if all are true:

1. Behavior is consistent with project README semantics.
2. Behavior remains aligned with MSSQLand intent unless a documented exception exists.
3. Impacket-dependent changes were validated against Impacket internals.
4. New actions are discoverable through registration/import flow.
5. Relevant tests were updated or added.
