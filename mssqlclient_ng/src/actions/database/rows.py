# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils.formatters import OutputFormatter


@ActionFactory.register("rows", "Retrieve all rows from a specified table")
class Rows(BaseAction):
    """
    Retrieves all rows from a specified table.

    Supports multiple formats:
    - table: Uses current database and user's default schema
    - schema.table: Uses current database with specified schema
    - database.schema.table: Fully qualified table name
    - database..table: Uses specified database with user's default schema

    Optional arguments:
    - -l/--limit: Maximum number of rows to retrieve (default: no limit)
    """

    def __init__(self):
        super().__init__()
        self._fqtn: str = ""
        self._database: Optional[str] = None
        self._schema: Optional[str] = None  # None = use user's default schema
        self._table: str = ""
        self._limit: int = 0  # 0 = no limit

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the table name argument and optional flags.

        Args:
            additional_arguments: Table name or FQTN with optional --limit/-l flags

        Raises:
            ValueError: If the table name format is invalid.
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Rows action requires at least a Table Name as an argument or a "
                "Fully Qualified Table Name (FQTN) in the format 'database.schema.table'."
            )

        # Parse named and positional arguments
        named_args, positional_args = self._parse_action_arguments(
            additional_arguments.strip()
        )

        # Get the FQTN from the first positional argument
        if not positional_args:
            raise ValueError(
                "Rows action requires at least a Table Name as an argument or a "
                "Fully Qualified Table Name (FQTN) in the format 'database.schema.table'."
            )

        self._fqtn = positional_args[0]
        parts = self._fqtn.split(".")

        if len(parts) == 3:  # Format: database.schema.table
            self._database = parts[0]
            self._schema = parts[1] if parts[1] else None
            self._table = parts[2]
        elif len(parts) == 2:  # Format: schema.table
            self._database = None  # Use the current database
            self._schema = parts[0]
            self._table = parts[1]
        elif len(parts) == 1:  # Format: table
            self._database = None  # Use the current database
            self._schema = None  # Use user's default schema
            self._table = parts[0]
        else:
            raise ValueError(
                "Invalid format for the argument. Expected formats: "
                "'database.schema.table', 'schema.table', or 'table'."
            )

        if not self._table:
            raise ValueError("Table name cannot be empty.")

        # Parse limit argument
        if "limit" in named_args or "l" in named_args:
            limit_str = named_args.get("limit", named_args.get("l"))
            try:
                self._limit = int(limit_str)
                if self._limit < 0:
                    raise ValueError(
                        f"Invalid limit value: {self._limit}. Limit must be a non-negative integer."
                    )
            except ValueError as e:
                if "invalid literal" in str(e):
                    raise ValueError(
                        f"Invalid limit value: '{limit_str}'. Must be an integer."
                    )
                raise

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        """
        Executes the rows retrieval query.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            List of rows from the table.
        """
        # Use the execution database if no database is specified
        if not self._database:
            self._database = database_context.query_service.execution_database

        # Build the target table name based on what was specified
        if self._schema:
            target_table = f"[{self._database}].[{self._schema}].[{self._table}]"
        else:
            # No schema specified - let SQL Server use the user's default schema
            target_table = f"[{self._database}]..[{self._table}]"

        logger.info(f"Retrieving rows from {target_table}")

        if self._limit > 0:
            logger.info(f"Limiting to {self._limit} row(s)")

        # Build query with optional TOP
        query = "SELECT"

        if self._limit > 0:
            query += f" TOP ({self._limit})"

        query += f" * FROM {target_table};"

        try:
            rows = database_context.query_service.execute_table(query)

            if not rows:
                logger.warning("No rows found in the table")
                return []

            print(OutputFormatter.convert_list_of_dicts(rows))

            return rows

        except Exception as e:
            logger.error(f"Failed to retrieve rows from {target_table}: {e}")
            raise

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List containing the table name argument description.
        """
        return ["table|schema.table|database.schema.table [-l/--limit N]"]
