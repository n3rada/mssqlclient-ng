# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils.formatters import OutputFormatter


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
                                 Supports: database_name, -d database_name, --database database_name
        """
        if not additional_arguments or not additional_arguments.strip():
            # No database specified - will use current database
            return

        # Parse arguments using the base class method
        named_args, positional_args = self._parse_action_arguments(additional_arguments)

        # Position = 0, ShortName = "db", LongName = "database"
        self._database = (
            named_args.get("database")  # --database
            or named_args.get("db")  # -db (not standard but supported)
            or (positional_args[0] if positional_args else None)  # Position 0
        )

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        """
        Executes the tables enumeration.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            List of tables with their properties.
        """
        # Use the execution database if no database is specified
        if not self._database:
            self._database = database_context.query_service.execution_database

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
                t.type IN ('U', 'V')
                AND p.index_id IN (0, 1)
            GROUP BY
                s.name, t.name, t.type_desc
            ORDER BY
                SchemaName, TableName;
        """

        tables = database_context.query_service.execute_table(query)

        if not tables:
            logger.warning("No tables found.")
            return []

        # Get all permissions in a single query for better performance
        all_permissions_query = f"""
            USE [{self._database}];
            SELECT
                SCHEMA_NAME(o.schema_id) AS schema_name,
                o.name AS object_name,
                p.permission_name
            FROM sys.objects o
            CROSS APPLY fn_my_permissions(QUOTENAME(SCHEMA_NAME(o.schema_id)) + '.' + QUOTENAME(o.name), 'OBJECT') p
            WHERE o.type IN ('U', 'V')
            ORDER BY o.name, p.permission_name;
        """

        all_permissions = database_context.query_service.execute_table(
            all_permissions_query
        )

        # Build a dictionary for fast lookup: key = "schema.table", value = set of unique permissions
        permissions_dict = {}

        for perm_row in all_permissions:
            key = f"{perm_row['schema_name']}.{perm_row['object_name']}"
            permission = perm_row["permission_name"]

            if key not in permissions_dict:
                permissions_dict[key] = set()
            permissions_dict[key].add(permission)

        # Map permissions to tables
        for table in tables:
            schema_name = table["SchemaName"]
            table_name = table["TableName"]
            key = f"{schema_name}.{table_name}"

            if key in permissions_dict:
                table["Permissions"] = ", ".join(sorted(permissions_dict[key]))
            else:
                table["Permissions"] = ""

        print(OutputFormatter.convert_list_of_dicts(tables))

        logger.success(f"Retrieved {len(tables)} table(s) from [{self._database}]")

        return tables

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List of argument descriptions.
        """
        return ["[database | --database database | -db database]"]
