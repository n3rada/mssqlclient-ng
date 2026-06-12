# mssqlclient-ng: AI Engineering Guide

This file is the canonical AI guidance for this repository.
Architecture, design principles, and extension patterns live in [DEVELOPMENT.md](DEVELOPMENT.md).

## Runtime Requirement

- Python version baseline is 3.11+ (see [pyproject.toml](pyproject.toml): `requires-python = ">=3.11,<4.0"`).
- Do not introduce language features or dependencies incompatible with Python 3.11.
- Use `uv` for environment and dependency workflows in this repo.

## Read Order

1. [README.md](README.md) - CLI behavior, authentication modes, linked-server usage.
2. [DEVELOPMENT.md](DEVELOPMENT.md) - architecture, design principles, and extension model (source of truth).
3. [src/mssqlclient_ng/cli.py](src/mssqlclient_ng/cli.py) - runtime flow and top-level command handling.
4. [tests](tests) - expected behavior and regression boundaries.

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

## Git Conventions

- Commit messages must follow [Conventional Commits](https://www.conventionalcommits.org/): `type(scope): short description`. One-liner, no body unless strictly necessary.
- Always sign the commits with `-S`.
- Never use em-dashes in commit messages.
- Never add `Co-Authored-By:` trailers.

## Definition of Done

A change is complete only if all are true:

1. Behavior is consistent with project README semantics.
2. Behavior remains aligned with MSSQLand intent unless a documented exception exists.
3. Impacket-dependent changes were validated against Impacket internals.
4. New actions are discoverable through registration/import flow.
5. Relevant tests were updated or added.
