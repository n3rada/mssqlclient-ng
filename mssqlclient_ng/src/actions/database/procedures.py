# Built-in imports
from enum import Enum
from pathlib import Path
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils.formatters import OutputFormatter


class ProcedureMode(Enum):
    """Execution mode for the procedures action."""

    LIST = "list"
    EXEC = "exec"
    READ = "read"
    SEARCH = "search"
    CREATE = "create"


@ActionFactory.register(
    "procedures", "List, execute, read, search, or create stored procedures"
)
class Procedures(BaseAction):
    """
    Manages stored procedures in the database.

    Modes:
    - list: Lists all stored procedures with permissions (default)
    - exec <schema.procedure> [args]: Executes a stored procedure with optional arguments
    - read <schema.procedure>: Reads the definition of a stored procedure
    - search <keyword>: Searches for procedures containing a keyword in their definition
    - create <file_path> [database_name]: Creates a stored procedure from a SQL file
    """

    def __init__(self):
        super().__init__()
        self._mode: ProcedureMode = ProcedureMode.LIST
        self._procedure_name: Optional[str] = None
        self._procedure_args: str = ""
        self._search_keyword: Optional[str] = None
        self._procedure_file_path: Optional[Path] = None
        self._target_database: Optional[str] = None

    def _validate_procedure_format(self, procedure_name: str) -> None:
        """
        Validates that procedure name is in schema.procedure format.

        Args:
            procedure_name: The procedure name to validate

        Raises:
            ValueError: If the procedure name is not in schema.procedure format
        """
        if not procedure_name or "." not in procedure_name:
            raise ValueError(
                f"Procedure name must be in 'schema.procedure' format. Got: '{procedure_name}'"
            )

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the arguments for the procedures action.

        Args:
            additional_arguments: Mode and procedure name/args as needed

        Raises:
            ValueError: If arguments are invalid.
        """
        if not additional_arguments or not additional_arguments.strip():
            # Default to listing stored procedures
            return

        parts = additional_arguments.strip().split(None, 2)  # Split into max 3 parts

        command = parts[0].lower()

        if command == "list":
            self._mode = ProcedureMode.LIST
        elif command == "exec":
            if len(parts) < 2:
                raise ValueError(
                    "Missing procedure name. Example: procedures exec dbo.sp_GetUsers 'param1, param2'"
                )
            self._mode = ProcedureMode.EXEC
            self._procedure_name = parts[1]
            self._validate_procedure_format(self._procedure_name)
            self._procedure_args = parts[2] if len(parts) > 2 else ""
        elif command == "read":
            if len(parts) < 2:
                raise ValueError(
                    "Missing procedure name. Example: procedures read dbo.sp_GetUsers"
                )
            self._mode = ProcedureMode.READ
            self._procedure_name = parts[1]
            self._validate_procedure_format(self._procedure_name)
        elif command == "search":
            if len(parts) < 2:
                raise ValueError(
                    "Missing search keyword. Example: procedures search EXEC"
                )
            self._mode = ProcedureMode.SEARCH
            self._search_keyword = parts[1]
        elif command == "create":
            if len(parts) < 2:
                raise ValueError(
                    "Missing file path. Example: procedures create ./path/to/procedure.sql [database_name]"
                )
            self._mode = ProcedureMode.CREATE
            self._procedure_file_path = Path(parts[1])
            if not self._procedure_file_path.exists():
                raise ValueError(f"File not found: {self._procedure_file_path}")
            if not self._procedure_file_path.is_file():
                raise ValueError(f"Path is not a file: {self._procedure_file_path}")
            self._target_database = parts[2] if len(parts) > 2 else None
        else:
            raise ValueError(
                "Invalid mode. Use 'list', 'exec <schema.procedure> [args]', 'read <schema.procedure>', 'search <keyword>', or 'create <file_path> [database_name]'"
            )

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        """
        Executes the procedures action based on the selected mode.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            Result data or None depending on the mode.
        """
        if self._mode == ProcedureMode.LIST:
            return self._list_procedures(database_context)
        elif self._mode == ProcedureMode.EXEC:
            return self._execute_procedure(database_context)
        elif self._mode == ProcedureMode.READ:
            return self._read_procedure_definition(database_context)
        elif self._mode == ProcedureMode.SEARCH:
            return self._search_procedures(database_context)
        elif self._mode == ProcedureMode.CREATE:
            return self._create_procedure(database_context)
        else:
            logger.error("Unknown execution mode")
            return None

    def _list_procedures(self, database_context: DatabaseContext) -> list[dict]:
        """
        Lists all stored procedures in the database with permissions.

        Args:
            database_context: The DatabaseContext instance.

        Returns:
            List of stored procedures.
        """
        exec_db = database_context.query_service.execution_database
        logger.info(f"Retrieving all stored procedures in [{exec_db}]")

        query = """
            SELECT
                SCHEMA_NAME(p.schema_id) AS [Schema],
                p.name AS [Name],
                USER_NAME(OBJECTPROPERTY(p.object_id, 'OwnerId')) AS [Owner],
                CASE
                    WHEN m.execute_as_principal_id IS NULL THEN ''
                    WHEN m.execute_as_principal_id = -2 THEN 'OWNER'
                    ELSE USER_NAME(m.execute_as_principal_id)
                END AS [ExecuteAsContext],
                p.create_date AS [Created],
                p.modify_date AS [Modified]
            FROM sys.procedures p
            INNER JOIN sys.sql_modules m ON p.object_id = m.object_id;
        """

        try:
            procedures = database_context.query_service.execute_table(query)

            if not procedures:
                logger.warning("No stored procedures found")
                return []

            # Get all permissions in a single query
            permissions_query = """
                SELECT
                    SCHEMA_NAME(o.schema_id) AS schema_name,
                    o.name AS object_name,
                    p.permission_name
                FROM sys.objects o
                CROSS APPLY fn_my_permissions(QUOTENAME(SCHEMA_NAME(o.schema_id)) + '.' + QUOTENAME(o.name), 'OBJECT') p
                WHERE o.type = 'P'
                ORDER BY o.name, p.permission_name;
            """

            all_permissions = database_context.query_service.execute_table(
                permissions_query
            )

            # Build a dictionary for fast lookup: key = "schema.procedure", value = list of permissions
            permissions_dict = {}
            for perm_row in all_permissions:
                key = f"{perm_row['schema_name']}.{perm_row['object_name']}"
                permission = perm_row["permission_name"]

                if key not in permissions_dict:
                    permissions_dict[key] = []
                permissions_dict[key].append(permission)

            # Add permissions to each procedure
            for proc in procedures:
                key = f"{proc['Schema']}.{proc['Name']}"
                if key in permissions_dict:
                    proc["Permissions"] = ", ".join(permissions_dict[key])
                else:
                    proc["Permissions"] = ""

            # Sort procedures by execution context, permissions, schema, name, and modified date
            def sort_key(proc):
                exec_context = proc.get("ExecuteAsContext", "")
                perms = proc.get("Permissions", "")

                # Priority for execution context (CALLER/OWNER first)
                exec_priority = 1 if exec_context in ("CALLER", "OWNER") else 0

                # Priority for permissions (EXECUTE > CONTROL > ALTER > others)
                if "EXECUTE" in perms:
                    perm_priority = 0
                elif "CONTROL" in perms:
                    perm_priority = 1
                elif "ALTER" in perms:
                    perm_priority = 2
                else:
                    perm_priority = 3

                return (
                    exec_priority,
                    perm_priority,
                    proc.get("Schema", ""),
                    proc.get("Name", ""),
                    proc.get("Modified", ""),
                )

            sorted_procedures = sorted(procedures, key=sort_key, reverse=False)

            print(OutputFormatter.convert_list_of_dicts(sorted_procedures))

            logger.info(f"Total: {len(sorted_procedures)} stored procedure(s) found")
            logger.warning(
                "Execution context depends on the statements used inside the stored procedure."
            )
            logger.warning(
                "Dynamic SQL executed with EXEC or sp_executesql runs under caller permissions by default."
            )
            logger.warning(
                "Static SQL inside a procedure uses ownership chaining, which may allow operations (e.g., SELECT) that the caller is not directly permitted to perform."
            )

            return sorted_procedures

        except Exception as e:
            logger.error(f"Failed to retrieve stored procedures: {e}")
            raise

    def _execute_procedure(
        self, database_context: DatabaseContext
    ) -> Optional[list[dict]]:
        """
        Executes a stored procedure with optional parameters.

        Args:
            database_context: The DatabaseContext instance.

        Returns:
            Result of the procedure execution.
        """
        exec_db = database_context.query_service.execution_database
        logger.info(f"Executing [{exec_db}].[{self._procedure_name}]")
        if self._procedure_args:
            logger.debug(f"With arguments: {self._procedure_args}")

        # Use schema-qualified name in EXEC, replace . with ].[
        qualified_name = self._procedure_name.replace(".", "].[")
        query = f"EXEC [{qualified_name}] {self._procedure_args};"

        try:
            result = database_context.query_service.execute_table(query)

            logger.success("Stored procedure executed successfully")

            if result:
                print(OutputFormatter.convert_list_of_dicts(result))
            else:
                logger.info("Procedure executed successfully with no result set")

            return result

        except Exception as e:
            logger.error(f"Error executing stored procedure: {e}")
            raise

    def _read_procedure_definition(
        self, database_context: DatabaseContext
    ) -> Optional[str]:
        """
        Reads the definition of a stored procedure.

        Args:
            database_context: The DatabaseContext instance.

        Returns:
            The procedure definition as a string.
        """
        exec_db = database_context.query_service.execution_database
        logger.info(f"Retrieving definition of [{exec_db}].[{self._procedure_name}]")

        # Parse schema.procedure format
        parts = self._procedure_name.split(".")
        schema = parts[0].replace("'", "''")
        procedure = parts[1].replace("'", "''")

        query = f"""
            SELECT
                m.definition
            FROM sys.sql_modules AS m
            INNER JOIN sys.objects AS o ON m.object_id = o.object_id
            INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id
            WHERE o.type = 'P'
            AND o.name = '{procedure}'
            AND s.name = '{schema}';
        """

        try:
            result = database_context.query_service.execute_table(query)

            if not result:
                logger.warning(f"Stored procedure '{self._procedure_name}' not found")
                return None

            definition = result[0].get("definition", "")

            if isinstance(definition, bytes):
                definition = definition.decode("utf-8", errors="replace")

            logger.success("Stored procedure definition retrieved")
            print(f"\n```sql\n{definition}\n```\n")

            return definition

        except Exception as e:
            logger.error(f"Error retrieving stored procedure definition: {e}")
            raise

    def _search_procedures(
        self, database_context: DatabaseContext
    ) -> Optional[list[dict]]:
        """
        Searches for stored procedures containing a specific keyword in their definition.

        Args:
            database_context: The DatabaseContext instance.

        Returns:
            List of procedures matching the search criteria.
        """
        exec_db = database_context.query_service.execution_database
        logger.info(
            f"Searching for keyword '{self._search_keyword}' in [{exec_db}] procedures"
        )

        # Escape single quotes in the keyword
        safe_keyword = self._search_keyword.replace("'", "''")

        query = f"""
            SELECT
                SCHEMA_NAME(o.schema_id) AS schema_name,
                o.name AS procedure_name,
                o.create_date,
                o.modify_date
            FROM sys.sql_modules AS m
            INNER JOIN sys.objects AS o ON m.object_id = o.object_id
            WHERE o.type = 'P'
            AND m.definition LIKE '%{safe_keyword}%'
            ORDER BY o.modify_date DESC;
        """

        try:
            result = database_context.query_service.execute_table(query)

            if not result:
                logger.warning(
                    f"No stored procedures found containing keyword '{self._search_keyword}'"
                )
                return []

            logger.success(
                f"Found {len(result)} stored procedure(s) containing '{self._search_keyword}'"
            )
            print(OutputFormatter.convert_list_of_dicts(result))

            logger.info(
                f"Total: {len(result)} stored procedure(s) matching search criteria"
            )

            return result

        except Exception as e:
            logger.error(f"Error searching stored procedures: {e}")
            raise

    def _create_procedure(self, database_context: DatabaseContext) -> None:
        """
        Creates a stored procedure in the database from a SQL file.

        Args:
            database_context: The DatabaseContext instance.

        Returns:
            None
        """
        target_db = self._target_database or "current database"
        logger.info(
            f"Creating stored procedure from file: {self._procedure_file_path} in {target_db}"
        )

        try:
            sql_content = self._procedure_file_path.read_text(encoding="utf-8")

            if not sql_content.strip():
                raise ValueError("SQL file is empty")

            if self._target_database:
                use_db_statement = f"USE [{self._target_database}];\n"
                sql_content = use_db_statement + sql_content

            database_context.query_service.execute(sql_content)

            logger.success(
                f"Stored procedure created successfully from {self._procedure_file_path.name} in {target_db}"
            )

            return None

        except Exception as e:
            logger.error(f"Error creating stored procedure: {e}")
            raise

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List of argument descriptions.
        """
        return [
            "[list|exec <schema.procedure> [args]|read <schema.procedure>|search <keyword>|create <file_path> [database_name]]"
        ]
