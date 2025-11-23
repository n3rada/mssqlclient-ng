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
    - table: Uses current database and dbo schema
    - schema.table: Uses current database with specified schema
    - database.schema.table: Fully qualified table name
    - database..table: Uses specified database with dbo schema
    """

    def __init__(self):
        super().__init__()
        self._fqtn: str = ""
        self._database: Optional[str] = None
        self._schema: str = "dbo"
        self._table: str = ""

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the table name argument.

        Args:
            additional_arguments: Table name or FQTN (database.schema.table)

        Raises:
            ValueError: If the table name format is invalid.
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Rows action requires at least a Table Name as an argument or a "
                "Fully Qualified Table Name (FQTN) in the format 'database.schema.table'."
            )

        self._fqtn = additional_arguments.strip()
        parts = self._fqtn.split(".")

        if len(parts) == 3:  # Format: database.schema.table
            self._database = parts[0]
            self._schema = parts[1] if parts[1] else "dbo"
            self._table = parts[2]
        elif len(parts) == 2:  # Format: schema.table
            self._database = None  # Use the current database
            self._schema = parts[0]
            self._table = parts[1]
        elif len(parts) == 1:  # Format: table
            self._database = None  # Use the current database
            self._schema = "dbo"  # Default schema
            self._table = parts[0]
        else:
            raise ValueError(
                "Invalid format for the argument. Expected formats: "
                "'database.schema.table', 'schema.table', or 'table'."
            )

        if not self._table:
            raise ValueError("Table name cannot be empty.")

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        """
        Executes the rows retrieval query.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            List of rows from the table.
        """
        # Use the current database if no database is specified
        if not self._database:
            self._database = database_context.query_service.get_current_database()

        target_table = f"[{self._database}].[{self._schema}].[{self._table}]"
        logger.info(f"Retrieving rows from {target_table}")

        query = f"SELECT * FROM {target_table};"

        try:
            rows = database_context.query_service.execute_table(query)

            if not rows:
                logger.warning("No rows found in the table")
                return []

            logger.success(f"Retrieved {len(rows)} row(s)")
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
        return ["table|schema.table|database.schema.table"]
