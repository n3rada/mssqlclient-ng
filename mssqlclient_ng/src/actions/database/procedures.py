# Built-in imports
from enum import Enum
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils import formatter


class ProcedureMode(Enum):
    """Execution mode for the procedures action."""

    LIST = "list"
    EXEC = "exec"
    READ = "read"


@ActionFactory.register(
    "procedures", "List, execute, or read stored procedure definitions"
)
class Procedures(BaseAction):
    """
    Manages stored procedures in the database.

    Modes:
    - list: Lists all stored procedures (default)
    - exec <procedure_name> [args]: Executes a stored procedure with optional arguments
    - read <procedure_name>: Reads the definition of a stored procedure
    """

    def __init__(self):
        super().__init__()
        self._mode: ProcedureMode = ProcedureMode.LIST
        self._procedure_name: Optional[str] = None
        self._procedure_args: str = ""

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
                    "Missing procedure name. Example: procedures exec sp_GetUsers 'param1, param2'"
                )
            self._mode = ProcedureMode.EXEC
            self._procedure_name = parts[1]
            self._procedure_args = parts[2] if len(parts) > 2 else ""
        elif command == "read":
            if len(parts) < 2:
                raise ValueError("Missing procedure name for reading definition.")
            self._mode = ProcedureMode.READ
            self._procedure_name = parts[1]
        else:
            raise ValueError(
                "Invalid mode. Use 'list', 'exec <procedure_name> [args]', or 'read <procedure_name>'"
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
        else:
            logger.error("Unknown execution mode")
            return None

    def _list_procedures(self, database_context: DatabaseContext) -> list[dict]:
        """
        Lists all stored procedures in the database.

        Args:
            database_context: The DatabaseContext instance.

        Returns:
            List of stored procedures.
        """
        logger.info("Retrieving all stored procedures in the database")

        query = """
            SELECT
                SCHEMA_NAME(schema_id) AS schema_name,
                name AS procedure_name,
                create_date,
                modify_date
            FROM sys.procedures
            ORDER BY modify_date DESC;
        """

        try:
            procedures = database_context.query_service.execute_table(query)

            if not procedures:
                logger.warning("No stored procedures found")
                return []

            logger.success(f"Found {len(procedures)} stored procedure(s)")
            print(formatter.rows_to_markdown_table(procedures))

            return procedures

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
        logger.info(f"Executing stored procedure: {self._procedure_name}")

        query = f"EXEC {self._procedure_name} {self._procedure_args};"

        try:
            result = database_context.query_service.execute_table(query)

            logger.success(f"Stored procedure '{self._procedure_name}' executed")

            if result:
                print(formatter.rows_to_markdown_table(result))
            else:
                logger.info("Procedure executed successfully with no result set")

            return result

        except Exception as e:
            logger.error(
                f"Error executing stored procedure '{self._procedure_name}': {e}"
            )
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
        logger.info(
            f"Retrieving definition of stored procedure: {self._procedure_name}"
        )

        query = f"""
            SELECT
                m.definition
            FROM sys.sql_modules AS m
            INNER JOIN sys.objects AS o ON m.object_id = o.object_id
            WHERE o.type = 'P' AND o.name = '{self._procedure_name}';
        """

        try:
            result = database_context.query_service.execute_table(query)

            if not result:
                logger.warning(f"Stored procedure '{self._procedure_name}' not found")
                return None

            definition = result[0].get("definition", "")

            if isinstance(definition, bytes):
                definition = definition.decode("utf-8", errors="replace")

            logger.success(
                f"Stored procedure '{self._procedure_name}' definition retrieved"
            )
            print(f"\n```sql\n{definition}\n```\n")

            return definition

        except Exception as e:
            logger.error(f"Error retrieving stored procedure definition: {e}")
            raise

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List of argument descriptions.
        """
        return ["[list|exec <procedure_name> [args]|read <procedure_name>]"]
