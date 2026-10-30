# Repository instructions

## Project

mssqlclient-ng is the Python/Impacket counterpart of MSSQLand for external SQL Server access, authentication, linked-server traversal, and cascading impersonation.

Read [`DEVELOPMENT.md`](DEVELOPMENT.md) when changing architecture, action behavior, Impacket/TDS use, authentication, query routing, impersonation, linked-server behavior, logging, output, or dependencies. For focused changes, read only the relevant section and its linked source files.

Before preparing a commit, pull the current branch with `git pull --rebase --autostash`; never commit against stale remote history. Then follow the human contribution and commit conventions in [`DEVELOPMENT.md`](DEVELOPMENT.md).

## Core contracts

- Maintain action and output parity with MSSQLand unless an Impacket or Python constraint requires a documented difference.
- Inspect relevant Impacket internals before changing TDS, authentication, or low-level protocol behavior.
- Keep SQL routing and wrapping in `QueryService`, identity and impersonation state in `DatabaseContext`, and capabilities in focused actions.
- Use the `Arg` descriptor, formatter pipeline, and action registration/import flow rather than duplicating them.
- Target Python 3.11 and use concrete modern type annotations.
- Preserve CLI, timeout, retry, fallback, and output semantics unless the task intentionally changes them.

## Verification

Run affected tests, or the full suite with `uv run pytest -v`.
