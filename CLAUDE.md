# mssqlclient-ng — AI Context

## Documentation

| File | Purpose |
|---|---|
| [README.md](README.md) | Usage, CLI syntax, linked server chain format, authentication modes, installation |

## Project

Python 3.9+, packaged with `uv` (build backend: `uv_build`). Entry point: [`src/mssqlclient_ng/cli.py`](src/mssqlclient_ng/cli.py)`:main`.

Dependencies: [Impacket](https://github.com/fortra/impacket) (TDS), [loguru](https://github.com/Delgan/loguru), [prompt_toolkit](https://github.com/prompt-toolkit/python-prompt-toolkit), [Pygments](https://pygments.org/).

```bash
# Dev install
uv sync

# Run
uv run mssqlclient-ng <host> [options]

# Tests
uv run pytest -v
```

## Architecture

```
src/mssqlclient_ng/
├── cli.py                  # Entry point, argument parsing, connection flow
├── banner.py               # ASCII banner
├── core/
│   ├── terminal.py         # Interactive shell (prompt_toolkit)
│   ├── actions/
│   │   ├── base.py         # BaseAction ABC
│   │   ├── factory.py      # ActionFactory with @register decorator
│   │   ├── administration/ # Server management, sessions, config
│   │   ├── agent/          # SQL Server Agent jobs
│   │   ├── configmgr/      # SCCM/ConfigMgr actions (cm_base.py base class)
│   │   ├── database/       # Core DB actions (whoami, info, tables, etc.)
│   │   ├── domain/         # AD interactions (ADSI, RID cycling)
│   │   ├── execution/      # Command execution (xp_cmdshell, OLE, CLR, etc.)
│   │   ├── filesystem/     # File read/write/upload/tree
│   │   └── remote/         # Linked servers, RPC, data access
│   ├── models/
│   │   ├── server.py       # Server model with parse_server()
│   │   └── linked_servers.py  # LinkedServers chain model
│   ├── services/
│   │   ├── database.py     # DatabaseContext facade
│   │   ├── query.py        # QueryService (execute, prepare, chain wrapping)
│   │   ├── user.py         # UserService (identity, impersonation)
│   │   ├── authentication.py  # AuthenticationService (NTLM, Kerberos, PTH)
│   │   ├── configuration.py   # ConfigurationService
│   │   ├── ntlmrelay.py    # NTLM relay listener
│   │   ├── adsi.py         # ADSI linked server helpers
│   │   └── configmgr.py    # ConfigMgr service
│   └── utils/
│       ├── logbook.py      # Loguru setup, file logging, Impacket log bridge
│       ├── completions.py  # Tab completers (actions + SQL keywords)
│       ├── formatters/     # OutputFormatter (markdown, csv)
│       ├── common.py       # yes_no_prompt, misc helpers
│       ├── helper.py       # Byte/encoding/network utilities
│       └── storage.py      # XDG-based data storage
tests/                      # pytest test suite
```

[`DatabaseContext`](src/mssqlclient_ng/core/services/database.py) is the single facade actions receive. It composes [`QueryService`](src/mssqlclient_ng/core/services/query.py), [`UserService`](src/mssqlclient_ng/core/services/user.py), and [`ConfigurationService`](src/mssqlclient_ng/core/services/configuration.py). Actions never handle impersonation or linked-server wrapping — [`QueryService._prepare_query()`](src/mssqlclient_ng/core/services/query.py) does it transparently.

## Adding a New Action

### 1. Create the file

```python
# src/mssqlclient_ng/core/actions/<category>/my_action.py

from typing import Optional
from loguru import logger

from ..base import BaseAction          # src/mssqlclient_ng/core/actions/base.py
from ..factory import ActionFactory    # src/mssqlclient_ng/core/actions/factory.py
from ...services.database import DatabaseContext  # src/mssqlclient_ng/core/services/database.py
from ...utils.formatters import OutputFormatter   # src/mssqlclient_ng/core/utils/formatters/


@ActionFactory.register("my-action", "One-line description for help", aliases=["ma"])
class MyAction(BaseAction):
    """Detailed docstring shown in !help my-action."""

    def validate_arguments(self, additional_arguments: str = "", argument_list=None) -> None:
        # Parse with self.parse_arguments(additional_arguments) or argparse
        named, positional = self.parse_arguments(additional_arguments)
        self._target = positional[0] if positional else None
        if not self._target:
            raise ValueError("Target argument is required")

    def execute(self, database_context: DatabaseContext) -> Optional[object]:
        query = f"SELECT name FROM sys.tables WHERE name LIKE '%{self._target}%'"
        rows = database_context.query_service.execute_table(query)

        if not rows:
            logger.warning("No results.")
            return None

        print(OutputFormatter.format_rows(rows))
        logger.success(f"{len(rows)} row(s) returned.")
        return rows
```

### 2. Import in `__init__.py`

Add to [`src/mssqlclient_ng/core/actions/<category>/__init__.py`](src/mssqlclient_ng/core/actions/) so the `@register` decorator fires at import time:

```python
from . import my_action  # noqa: F401
```

No manual registration in a dictionary — the decorator handles it.

## QueryService API — [`src/mssqlclient_ng/core/services/query.py`](src/mssqlclient_ng/core/services/query.py)

| Method | Returns | Use for |
|---|---|---|
| `execute_table(query)` | `List[Dict[str, Any]]` | SELECT queries returning rows |
| `execute_scalar(query)` | `Optional[Any]` | Single value (COUNT, @@SERVERNAME, etc.) |
| `execute(query)` | Raw TDS result | Low-level access |
| `execute_non_processing(query)` | `int` (affected rows) | INSERT/UPDATE/DELETE, DDL |

## Terminal Built-in Commands — [`src/mssqlclient_ng/core/terminal.py`](src/mssqlclient_ng/core/terminal.py)

| Command | Alias | Purpose |
|---|---|---|
| `!help [term]` | `!h` | List/filter actions or show action help |
| `!link <spec>` | — | Set linked server chain |
| `!unlink` | `!ul` | Pop last server from chain |
| `!unlink-all` | `!ula` | Clear entire chain |
| `!add-link <spec>` | `!al` | Append server to chain |
| `!impersonate <login>` | `!imp` | Impersonate a login |
| `!revert` | `!rev` | Revert impersonation |
| `!chain` | — | Display current chain with impersonation |
| `!format [name]` | — | Show/change output format |
| `!debug` | — | Toggle debug logging |

## Logging

Uses [loguru](https://github.com/Delgan/loguru) directly — no custom wrapper:

```python
from loguru import logger

logger.info("Informational")       # [i]
logger.success("Done")             # [✓]
logger.warning("Watch out")        # [!]
logger.error("Failed")             # [✗]
logger.debug("Verbose detail")     # Only shown in debug mode
```

## Key Differences from MSSQLand (C#)

- **Action registration**: Decorator `@ActionFactory.register()` instead of manual dictionary entry.
- **Argument parsing**: Manual (`self.parse_arguments()` or `argparse`) instead of reflection-based `[ArgumentMetadata]`.
- **Query results**: `execute_table()` returns `List[Dict]` (not `DataTable`).
- **No `.csproj`**: Just add the import in `__init__.py` and the decorator handles the rest.

## Key Conventions

- Every action module must be imported in its category's `__init__.py` to trigger `@register`.
- Actions receive `DatabaseContext` and call `query_service.execute_table()` / `execute_scalar()` — never handle chain wrapping manually.
- ConfigMgr actions inherit from [`CMBaseAction`](src/mssqlclient_ng/core/actions/configmgr/cm_base.py) instead of `BaseAction` directly.
- Output goes through [`OutputFormatter`](src/mssqlclient_ng/core/utils/formatters/) for consistent markdown/csv formatting.
- Tests live in `tests/` and run with `uv run pytest`.
