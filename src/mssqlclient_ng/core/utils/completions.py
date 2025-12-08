# mssqlclient_ng/core/utils/completions.py

# External library imports
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.document import Document

# Local library imports
from ..actions.factory import ActionFactory


# SQL keywords and built-in functions for autocompletion
SQL_KEYWORDS = [
    "SELECT",
    "FROM",
    "WHERE",
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "DROP",
    "ALTER",
    "TABLE",
    "DATABASE",
    "INDEX",
    "VIEW",
    "PROCEDURE",
    "FUNCTION",
    "TRIGGER",
    "SCHEMA",
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
    "LIMIT",
    "TOP",
    "OFFSET",
    "FETCH",
    "NEXT",
    "ROWS",
    "ONLY",
    "DISTINCT",
    "AS",
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
    "ALL",
    "INTO",
    "VALUES",
    "SET",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "TRANSACTION",
    "EXEC",
    "EXECUTE",
    "DECLARE",
    "PRINT",
    "IF",
    "WHILE",
    "RETURN",
    "TRY",
    "CATCH",
    "THROW",
    "RAISERROR",
    "GO",
    "USE",
    "GRANT",
    "REVOKE",
    "DENY",
    "WITH",
    "NOLOCK",
    "READPAST",
    "UPDLOCK",
    "ROWLOCK",
    "TABLOCK",
    "OPENQUERY",
    "OPENROWSET",
    "PIVOT",
    "UNPIVOT",
    "MERGE",
    "OUTPUT",
    "INSERTED",
    "DELETED",
    "WAITFOR",
    "DELAY",
    "TIME",
    "BACKUP",
    "RESTORE",
    "RECONFIGURE",
    "DBCC",
    "CHECKPOINT",
    "BULK",
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
    "TRUNCATE",
    "COLLATE",
]

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

SQL_SYSTEM_OBJECTS = [
    "sys.databases",
    "sys.tables",
    "sys.columns",
    "sys.views",
    "sys.procedures",
    "sys.objects",
    "sys.indexes",
    "sys.triggers",
    "sys.schemas",
    "sys.types",
    "sys.server_principals",
    "sys.database_principals",
    "sys.database_permissions",
    "sys.server_permissions",
    "sys.database_role_members",
    "sys.server_role_members",
    "sys.dm_exec_sessions",
    "sys.dm_exec_connections",
    "sys.dm_exec_requests",
    "sys.dm_exec_query_stats",
    "sys.dm_exec_sql_text",
    "sys.dm_exec_cached_plans",
    "sys.configurations",
    "sys.servers",
    "sys.linked_logins",
    "sys.syslogins",
    "sys.sql_logins",
    "sys.sysusers",
    "sys.sysprocesses",
    "sys.sysconfigures",
    "sys.sysdatabases",
    "master",
    "tempdb",
    "model",
    "msdb",
    "INFORMATION_SCHEMA.TABLES",
    "INFORMATION_SCHEMA.COLUMNS",
    "INFORMATION_SCHEMA.VIEWS",
    "INFORMATION_SCHEMA.ROUTINES",
    "sp_configure",
    "sp_executesql",
    "sp_addlinkedserver",
    "sp_droplinkedserver",
    "sp_serveroption",
    "sp_linkedservers",
    "sp_helptext",
    "sp_who",
    "sp_who2",
    "sp_helprotect",
    "xp_cmdshell",
    "xp_dirtree",
    "xp_fileexist",
    "xp_regread",
    "xp_regenumvalues",
]


class ActionCompleter(Completer):
    """
    Auto-completer for action commands.
    Suggests available actions when user starts typing with prefix.
    """

    def __init__(self, prefix: str = "!"):
        self.prefix = prefix

        # Built-in commands with descriptions
        self.builtins = {
            "exit": "Exit the terminal",
            "debug": "Toggle debug mode on/off",
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
            command_part = text[len(self.prefix) :].strip()

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

            # Also suggest built-in commands
            for builtin_name, builtin_desc in self.builtins.items():
                if builtin_name.startswith(command_part.lower()):
                    completion_text = builtin_name[len(command_part) :]
                    yield Completion(completion_text, 0, display_meta=builtin_desc)


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

        # Complete system objects (case-insensitive match)
        for obj in SQL_SYSTEM_OBJECTS:
            if obj.upper().startswith(word_upper):
                yield Completion(
                    obj, start_position=-len(word_before_cursor), display_meta="system"
                )
