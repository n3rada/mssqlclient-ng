# mssqlclient_ng/core/actions/database/rows.py

# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register("rows", "Retrieve all rows from a specified table")
class Rows(BaseAction):
    """
    Retrieves all rows from a specified table.

    Supports multiple formats:
    - table: Uses current database and dbo schema (default)
    - schema.table: Uses current database with specified schema
    - database.schema.table: Fully qualified table name

    Optional arguments:
    - -t/--top: Maximum number of rows to retrieve (default: no limit)
    """

    def __init__(self):
        super().__init__()
        self._fqtn: str = ""
        self._database: Optional[str] = None
        self._schema: str = "dbo"  # Default to dbo schema
        self._table: str = ""
        self._top: int = 0  # 0 = no limit

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the table name argument and optional flags.

        Args:
            additional_arguments: Table name or FQTN with optional --top/-t flags

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
            self._schema = parts[1]
            self._table = parts[2]
        elif len(parts) == 2:  # Format: schema.table
            self._database = None  # Use the current database
            self._schema = parts[0]
            self._table = parts[1]
        elif len(parts) == 1:  # Format: table
            self._database = None  # Use the current database
            self._schema = "dbo"  # Default to dbo schema
            self._table = parts[0]
        else:
            raise ValueError(
                "Invalid format. Use: [table], [schema.table], or [database.schema.table]."
            )

        if not self._table:
            raise ValueError("Table name cannot be empty.")

        # Parse top argument (supports both --top and -t)
        if "top" in named_args or "t" in named_args:
            top_str = named_args.get("top", named_args.get("t"))
            try:
                self._top = int(top_str)
                if self._top < 0:
                    raise ValueError(
                        f"Invalid top value: {self._top}. Top must be a non-negative integer."
                    )
            except ValueError as e:
                if "invalid literal" in str(e).lower():
                    raise ValueError(
                        f"Invalid top value: '{top_str}'. Must be an integer."
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

        # Build the target table name with all three parts
        target_table = f"[{self._database}].[{self._schema}].[{self._table}]"

        logger.info(f"Retrieving rows from {target_table}")

        if self._top > 0:
            logger.info(f"Limiting to {self._top} row(s)")

        # Build query with optional TOP
        query = "SELECT"

        if self._top > 0:
            query += f" TOP ({self._top})"

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
        return ["table|schema.table|database.schema.table [-t/--top N]"]
