# mssqlclient-ng: Development Guide

This file is the authoritative reference for architecture, design, and extension patterns.
It is read by both human contributors and AI agents. If this file and [AI.md](AI.md) ever conflict, this file takes precedence for architecture and design decisions.

## Source Map

- CLI entry point: [src/mssqlclient_ng/cli.py](src/mssqlclient_ng/cli.py)
- Interactive shell: [src/mssqlclient_ng/core/terminal.py](src/mssqlclient_ng/core/terminal.py)
- Action base and arg model: [src/mssqlclient_ng/core/actions/base.py](src/mssqlclient_ng/core/actions/base.py)
- Action registration: [src/mssqlclient_ng/core/actions/factory.py](src/mssqlclient_ng/core/actions/factory.py)
- DatabaseContext facade: [src/mssqlclient_ng/core/services/database.py](src/mssqlclient_ng/core/services/database.py)
- Query execution and wrapping: [src/mssqlclient_ng/core/services/query.py](src/mssqlclient_ng/core/services/query.py)
- Auth service: [src/mssqlclient_ng/core/services/authentication.py](src/mssqlclient_ng/core/services/authentication.py)
- Linked server model: [src/mssqlclient_ng/core/models/linked_servers.py](src/mssqlclient_ng/core/models/linked_servers.py)
- Formatting pipeline: [src/mssqlclient_ng/core/utils/formatters](src/mssqlclient_ng/core/utils/formatters)

## Python Rules

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
- Log level guide:
  - `logger.trace(...)`: developer-level internal mechanics — loop iteration detail, cache hits, retry counters, raw query routing. Invisible to operators by default. Intended for developers and AI-assisted diagnosis only, not for operator use.
  - `logger.debug(...)`: operator-facing detail useful to diagnose usage problems — skipped servers, failed impersonation attempts, negative-cache hits. Shown with `--debug`.
  - `logger.info(...)`: normal operational progress — connection milestones, chain discovery, link counts.
  - `logger.success(...)`: positive completion events — chain found, authentication succeeded.
  - `logger.warning(...)`: unexpected but recoverable state — no linked servers found, query fallback triggered.
  - `logger.error(...)`: hard failures that abort the current operation.

4. Code hygiene
- Keep comments concise and non-obvious.
- Prefer minimal diffs over broad refactors.
- Do not introduce repo-wide style churn while solving local problems.

## Design Principles

### 1. Single Responsibility (SRP)

Each module owns one concern. Violations are a signal to refactor, not work around.

- Actions in [src/mssqlclient_ng/core/actions](src/mssqlclient_ng/core/actions) express one user-facing capability each. They do not decide how SQL is routed or wrapped.
- Chain wrapping, `OPENQUERY`/`EXEC AT` selection, and linked-server traversal belong exclusively in [src/mssqlclient_ng/core/services/query.py](src/mssqlclient_ng/core/services/query.py).
- Identity, impersonation state, and execution context belong in [src/mssqlclient_ng/core/services/database.py](src/mssqlclient_ng/core/services/database.py).

### 2. Open/Closed

The action registry is the extension point. Existing contracts must not be broken to add new behavior.

- New actions are added by creating a module and decorating with `@ActionFactory.register(...)`. No existing code changes required.
- New formatters are added under [src/mssqlclient_ng/core/utils/formatters](src/mssqlclient_ng/core/utils/formatters) without touching the formatter dispatcher.
- If you find yourself editing `base.py` or `factory.py` to accommodate a single new action, reconsider the design.

### 3. Composition over Inheritance

Services are consumed, not subclassed.

- Actions receive a `DatabaseContext` instance in `execute()` and call services through it. They do not inherit from service classes or access internals directly.
- Prefer adding a method to an existing service over subclassing it or duplicating logic in an action.

### 4. DRY and Consistency

Shared utilities exist for a reason. Use them.

- Argument parsing: use the `Arg` descriptor pattern from [src/mssqlclient_ng/core/actions/base.py](src/mssqlclient_ng/core/actions/base.py). Do not hand-roll argument parsing in actions.
- Output formatting: route through [src/mssqlclient_ng/core/utils/formatters](src/mssqlclient_ng/core/utils/formatters). Do not build ad-hoc table strings in actions.
- SQL wrapping: always delegate to [src/mssqlclient_ng/core/services/query.py](src/mssqlclient_ng/core/services/query.py). Do not construct `OPENQUERY` or `EXEC AT` strings inside actions.

### 5. Fail Fast and Observable

Errors should surface immediately with enough context to act on them.

- Use `logger.error(...)` for hard failures, `logger.warning(...)` for recoverable degraded states.
- Do not swallow exceptions silently. If a fallback is necessary, log the original failure first.
- Keep operational semantics predictable: timeouts, retries, and chain fallback paths must behave consistently regardless of which action triggered them.

### 6. KISS

Prefer the simplest implementation that satisfies the requirement.

- Do not add abstraction layers for hypothetical future needs.
- Three similar lines in different actions is acceptable. Extract only when a fourth case appears or when the duplication creates a correctness risk.
- If a new action can be implemented entirely within one module without touching shared infrastructure, that is the correct scope.

## Action Authoring Rules

When adding or changing an action:

1. Create module under the correct category in [src/mssqlclient_ng/core/actions](src/mssqlclient_ng/core/actions).
2. Register with `@ActionFactory.register(...)`.
3. Keep argument declaration and validation consistent with [src/mssqlclient_ng/core/actions/base.py](src/mssqlclient_ng/core/actions/base.py).
4. Ensure category `__init__.py` imports the new module so registration executes.
5. Route SQL execution through [src/mssqlclient_ng/core/services/query.py](src/mssqlclient_ng/core/services/query.py), not ad-hoc wrappers.
6. For ConfigMgr actions, follow [src/mssqlclient_ng/core/actions/configmgr/cm_base.py](src/mssqlclient_ng/core/actions/configmgr/cm_base.py).

## Testing and Quality Gates

- Run tests with `uv run pytest -v`.
- Keep output stable for markdown/csv formatters.
- Preserve backwards-compatible CLI behavior unless a breaking change is explicitly intended.
