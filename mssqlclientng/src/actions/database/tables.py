# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from mssqlclientng.src.actions.base import BaseAction
from mssqlclientng.src.actions.factory import ActionFactory
from mssqlclientng.src.services.database import DatabaseContext
from mssqlclientng.src.utils import formatter


@ActionFactory.register(
    "tables", "List tables in a database with schemas and permissions"
)
class Tables(BaseAction):
    """
    Retrieves all tables and views from a database with row counts and permissions.

    Shows schema, table name, type, row count, and user permissions for each table.
    """

    def __init__(self):
        super().__init__()
        self._database: Optional[str] = None

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the database argument.

        Args:
            additional_arguments: Database name (optional, uses current database if not specified)
        """
        if additional_arguments and additional_arguments.strip():
            self._database = additional_arguments.strip()

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        """
        Executes the tables enumeration.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            List of tables with their properties.
        """
        # Use the current database if no database is specified
        if not self._database:
            self._database = database_context.server.database

        logger.info(f"Retrieving tables from [{self._database}]")

        query = f"""
            SELECT
                s.name AS SchemaName,
                t.name AS TableName,
                t.type_desc AS TableType,
                SUM(p.rows) AS Rows
            FROM
                [{self._database}].sys.objects t
            JOIN
                [{self._database}].sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN
                [{self._database}].sys.partitions p ON t.object_id = p.object_id
            WHERE
                t.type IN ('U', 'V') -- 'U' for user tables, 'V' for views
                AND p.index_id IN (0, 1) -- 0 for heaps, 1 for clustered index
            GROUP BY
                s.name, t.name, t.type_desc
            ORDER BY
                SchemaName, TableName;
        """

        tables = database_context.query_service.execute_table(query)

        if not tables:
            logger.warning(f"No tables found in database [{self._database}]")
            return None

        # Add permissions for each table
        for table in tables:
            schema_name = table["SchemaName"]
            table_name = table["TableName"]

            # Query to get user permissions on the table
            permission_query = f"""
                USE [{self._database}];
                SELECT DISTINCT
                    permission_name
                FROM
                    fn_my_permissions('[{schema_name}].[{table_name}]', 'OBJECT');
            """

            try:
                permission_result = database_context.query_service.execute_table(
                    permission_query
                )

                if permission_result:
                    # Concatenate permissions as a comma-separated string
                    permissions = ", ".join(
                        [p["permission_name"] for p in permission_result]
                    )
                else:
                    permissions = ""

                # Add permissions to the result
                table["Permissions"] = permissions

            except Exception as ex:
                logger.debug(
                    f"Failed to get permissions for [{schema_name}].[{table_name}]: {ex}"
                )
                table["Permissions"] = "Error"

        print(formatter.rows_to_markdown_table(tables))

        return tables

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List of argument descriptions.
        """
        return ["[database]"]
