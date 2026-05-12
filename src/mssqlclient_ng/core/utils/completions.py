# mssqlclient_ng/core/utils/completions.py

# Built-in imports
from typing import Callable, List, Optional, Tuple

# External library imports
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.document import Document

# Local library imports
from ..actions.factory import ActionFactory

# SQL keywords and built-in functions for autocompletion
# Organised into semantic groups so SQL_KEYWORDS and TSQL_STARTERS are derived,
# not duplicated.

# DML/DDL/control-flow words that open a T-SQL statement
_TSQL_STARTER_WORDS = [
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "TRUNCATE",
    "CREATE",
    "DROP",
    "ALTER",
    "EXEC",
    "EXECUTE",
    "WITH",
    "USE",
    "DECLARE",
    "SET",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "IF",
    "WHILE",
    "RETURN",
    "PRINT",
    "GOTO",
    "GRANT",
    "REVOKE",
    "DENY",
    "BACKUP",
    "RESTORE",
    "DBCC",
    "CHECKPOINT",
    "RECONFIGURE",
    "BULK",
    "RAISERROR",
    "THROW",
]

# Clause / mid-statement keywords
_TSQL_CLAUSE_WORDS = [
    "FROM",
    "WHERE",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "OUTER",
    "FULL",
    "CROSS",
    "APPLY",
    "ON",
    "GROUP",
    "BY",
    "HAVING",
    "ORDER",
    "ASC",
    "DESC",
    "TOP",
    "OFFSET",
    "FETCH",
    "NEXT",
    "ROWS",
    "ONLY",
    "DISTINCT",
    "AS",
    "ALL",
    "INTO",
    "VALUES",
    "OUTPUT",
    "INSERTED",
    "DELETED",
    "AND",
    "OR",
    "NOT",
    "IN",
    "BETWEEN",
    "LIKE",
    "IS",
    "NULL",
    "EXISTS",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "UNION",
    "INTERSECT",
    "EXCEPT",
    "TRANSACTION",
    "TRY",
    "CATCH",
    "GO",
    "WAITFOR",
    "DELAY",
    "TIME",
]

# Table hints
_TSQL_HINT_WORDS = [
    "NOLOCK",
    "READPAST",
    "UPDLOCK",
    "ROWLOCK",
    "TABLOCK",
]

# DDL object types and column/constraint modifiers
_TSQL_DDL_WORDS = [
    "TABLE",
    "DATABASE",
    "INDEX",
    "VIEW",
    "PROCEDURE",
    "FUNCTION",
    "TRIGGER",
    "SCHEMA",
    "IDENTITY",
    "PRIMARY",
    "KEY",
    "FOREIGN",
    "REFERENCES",
    "CONSTRAINT",
    "CHECK",
    "DEFAULT",
    "UNIQUE",
    "CLUSTERED",
    "NONCLUSTERED",
    "COLLATE",
]

# Rowset / special functions used as table sources
_TSQL_ROWSET_WORDS = [
    "OPENQUERY",
    "OPENROWSET",
    "PIVOT",
    "UNPIVOT",
]

# SQL_KEYWORDS is the union of all groups (used for autocompletion)
SQL_KEYWORDS: list = (
    _TSQL_STARTER_WORDS
    + _TSQL_CLAUSE_WORDS
    + _TSQL_HINT_WORDS
    + _TSQL_DDL_WORDS
    + _TSQL_ROWSET_WORDS
)

# Fast lookup set for the terminal T-SQL guard (comment openers included)
TSQL_STARTERS: frozenset = frozenset(w.lower() for w in _TSQL_STARTER_WORDS) | {
    "--",
    "/*",
}

# Starters that are valid as a complete single-token statement.
# Everything else in TSQL_STARTERS requires at least one more token.
TSQL_VALID_STANDALONE: frozenset = frozenset(
    w.lower() for w in ["COMMIT", "ROLLBACK", "CHECKPOINT", "RECONFIGURE", "RETURN"]
)

SQL_FUNCTIONS = [
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "CAST",
    "CONVERT",
    "ISNULL",
    "COALESCE",
    "NULLIF",
    "SUBSTRING",
    "LEN",
    "DATALENGTH",
    "UPPER",
    "LOWER",
    "LTRIM",
    "RTRIM",
    "TRIM",
    "REPLACE",
    "CHARINDEX",
    "PATINDEX",
    "LEFT",
    "RIGHT",
    "GETDATE",
    "GETUTCDATE",
    "SYSDATETIME",
    "SYSUTCDATETIME",
    "DATEADD",
    "DATEDIFF",
    "DATEDIFF_BIG",
    "DATEPART",
    "DATENAME",
    "YEAR",
    "MONTH",
    "DAY",
    "EOMONTH",
    "NEWID",
    "NEWSEQUENTIALID",
    "RAND",
    "ROUND",
    "ABS",
    "CEILING",
    "FLOOR",
    "POWER",
    "SQRT",
    "SQUARE",
    "EXP",
    "LOG",
    "LOG10",
    "SIGN",
    "CONCAT",
    "CONCAT_WS",
    "FORMAT",
    "STRING_AGG",
    "STRING_SPLIT",
    "STUFF",
    "REVERSE",
    "REPLICATE",
    "SPACE",
    "QUOTENAME",
    "ROW_NUMBER",
    "RANK",
    "DENSE_RANK",
    "NTILE",
    "LAG",
    "LEAD",
    "FIRST_VALUE",
    "LAST_VALUE",
    "CUME_DIST",
    "PERCENT_RANK",
    "STDEV",
    "STDEVP",
    "VAR",
    "VARP",
    "CHECKSUM",
    "BINARY_CHECKSUM",
    "HASHBYTES",
    "COMPRESS",
    "DECOMPRESS",
    "OBJECT_ID",
    "OBJECT_NAME",
    "SCHEMA_ID",
    "SCHEMA_NAME",
    "DB_ID",
    "DB_NAME",
    "USER_ID",
    "USER_NAME",
    "SUSER_SID",
    "SUSER_SNAME",
    "SUSER_NAME",
    "IS_MEMBER",
    "IS_ROLEMEMBER",
    "IS_SRVROLEMEMBER",
    "HAS_PERMS_BY_NAME",
    "@@VERSION",
    "@@SERVERNAME",
    "@@SERVICENAME",
    "@@IDENTITY",
    "@@ROWCOUNT",
    "@@ERROR",
    "@@TRANCOUNT",
    "SCOPE_IDENTITY",
    "IDENT_CURRENT",
    "IDENT_SEED",
    "IDENT_INCR",
]


class ActionCompleter(Completer):
    """
    Auto-completer for action commands.
    Suggests available actions when user starts typing with prefix.
    """

    # Commands that accept a chain ID as their first argument
    _CHAIN_ID_COMMANDS = {"chain", "ch", "tunnel", "linkmap", "linksmap", "chains"}

    def __init__(
        self,
        prefix: str = "!",
        chain_loader: Optional[Callable[[], List[Tuple[int, str]]]] = None,
    ):
        """
        Args:
            prefix: The command prefix character (default "!")
            chain_loader: Optional callable returning [(id, summary), ...] for
                          chain ID completion. Called lazily on each completion.
        """
        self.prefix = prefix
        self._chain_loader = chain_loader

        # Built-in commands with descriptions
        self.builtins = {
            "exit": "Exit the terminal",
            "debug": "Toggle debug mode on/off",
            "chain": "Display full connection chain with impersonation context",
            "link": "Set linked server chain (e.g. SQL02/user;SQL03@db)",
            "unlink": "Remove last server from chain",
            "unlink-all": "Clear entire linked server chain",
            "add-link": "Add server to chain (e.g. SQL03/user@db)",
            "impersonate": "Impersonate a login on current connection (EXECUTE AS)",
            "revert": "Revert impersonation on current connection (REVERT)",
        }

        # Aliases mapping to canonical command names
        self.aliases = {
            "imp": "impersonate",
            "rev": "revert",
            "ul": "unlink",
            "ula": "unlink-all",
            "al": "add-link",
        }

    def get_completions(self, document: Document, complete_event):
        """
        Generate completion suggestions.

        Args:
            document: The document being edited
            complete_event: The completion event

        Yields:
            Completion objects for matching actions with descriptions
        """
        text = document.text_before_cursor

        # Check if we're at the start with prefix
        if text.startswith(self.prefix):
            # Get the part after the prefix
            command_part = text[len(self.prefix) :].lstrip()

            # Check if the user has already typed a complete command and a space,
            # meaning they are now typing the argument to that command.
            parts = command_part.split(maxsplit=1)
            if len(parts) == 2 or (len(parts) == 1 and text.endswith(" ")):
                cmd = parts[0].lower()
                arg_prefix = parts[1] if len(parts) == 2 else ""
                if cmd in self._CHAIN_ID_COMMANDS and self._chain_loader is not None:
                    yield from self._chain_id_completions(arg_prefix)
                    return

            # Get all available actions
            actions = ActionFactory.list_actions()

            # Filter actions that match what the user has typed
            for action_name in actions:
                if action_name.startswith(command_part.lower()):
                    # Calculate how much we need to replace
                    completion_text = action_name[len(command_part) :]

                    # Get the description for this action
                    description = ActionFactory.get_action_description(action_name)
                    help_text = f"{description}" if description else ""

                    yield Completion(completion_text, 0, display_meta=help_text)

            # Also suggest action aliases
            for alias, canonical in ActionFactory.list_aliases().items():
                if alias.startswith(command_part.lower()):
                    completion_text = alias[len(command_part) :]
                    yield Completion(completion_text, 0, display_meta=f"→ {canonical}")

            # Also suggest built-in commands
            for builtin_name, builtin_desc in self.builtins.items():
                if builtin_name.startswith(command_part.lower()):
                    completion_text = builtin_name[len(command_part) :]
                    yield Completion(completion_text, 0, display_meta=builtin_desc)

            # Also suggest built-in command aliases
            for alias, canonical in self.aliases.items():
                if alias.startswith(command_part.lower()):
                    completion_text = alias[len(command_part) :]
                    yield Completion(completion_text, 0, display_meta=f"→ !{canonical}")

    def _chain_id_completions(self, arg_prefix: str):
        """Yield chain ID completions from the chain loader."""
        if self._chain_loader is None:
            return
        try:
            entries = self._chain_loader()
        except Exception:
            return
        for chain_id, summary in entries:
            id_str = str(chain_id)
            if id_str.startswith(arg_prefix):
                yield Completion(
                    id_str[len(arg_prefix) :],
                    start_position=0,
                    display=id_str,
                    display_meta=summary,
                )


class SQLBuiltinCompleter(Completer):
    """
    Auto-completer for SQL keywords, functions, and system objects.
    """

    def get_completions(self, document: Document, complete_event):
        """
        Generate SQL keyword and function completions.

        Args:
            document: The document being edited
            complete_event: The completion event

        Yields:
            Completion objects for matching SQL keywords and functions
        """
        text = document.text_before_cursor

        # Don't complete if user is typing an action command
        if text.lstrip().startswith("!"):
            return

        # Get the current word being typed
        word_before_cursor = document.get_word_before_cursor(WORD=True)
        word_upper = word_before_cursor.upper()

        # Complete SQL keywords
        for keyword in SQL_KEYWORDS:
            if keyword.startswith(word_upper):
                yield Completion(
                    keyword,
                    start_position=-len(word_before_cursor),
                    display_meta="keyword",
                )

        # Complete SQL functions
        for function in SQL_FUNCTIONS:
            if function.startswith(word_upper):
                yield Completion(
                    function + "(",
                    start_position=-len(word_before_cursor),
                    display_meta="function",
                )
