# /mssqlclient_ng/core/actions/database/tables.py

# Built-in imports
from typing import Optional, List, Dict

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatter import OutputFormatter


@ActionFactory.register(
    "tables",
    "List tables in a database with schemas and permissions",
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
        Validate the database argument.

        Args:
            additional_arguments: Optional database name argument
        """
        if not additional_arguments or not additional_arguments.strip():
            self._database = None
            return

        named_args, positional_args = self._parse_action_arguments(
            additional_arguments.strip()
        )

        # Get database from positional or named arguments
        if positional_args:
            self._database = positional_args[0]
        else:
            self._database = named_args.get("database") or named_args.get("db")

        # If still None, will use current database in Execute()

    def execute(self, database_context: DatabaseContext) -> Optional[List[Dict]]:
        """
        Execute the tables enumeration.

        Args:
            database_context: The DatabaseContext instance to execute the query

        Returns:
            List of tables with their properties
        """
        # Use the execution database if no database is specified
        target_database = (
            self._database
            if self._database
            else database_context.query_service.execution_database
        )

        logger.info(f"Retrieving tables from [{target_database}]")

        # Build USE statement if specific database is provided
        use_statement = f"USE [{self._database}];" if self._database else ""

        query = f"""
                {use_statement}
                SELECT
                    s.name AS SchemaName,
                    t.name AS TableName,
                    t.type_desc AS TableType,
                    SUM(p.rows) AS Rows
                FROM
                    sys.objects t
                JOIN
                    sys.schemas s ON t.schema_id = s.schema_id
                LEFT JOIN
                    sys.partitions p ON t.object_id = p.object_id
                WHERE
                    t.type IN ('U', 'V')
                    AND p.index_id IN (0, 1)
                GROUP BY
                    s.name, t.name, t.type_desc
                ORDER BY
                    SchemaName, TableName;"""

        tables = database_context.query_service.execute_table(query)

        if not tables:
            logger.warning("No tables found.")
            return tables

        # Get all permissions in a single query
        all_permissions_query = f"""
                {use_statement}
                SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name AS object_name, p.permission_name
                FROM sys.objects o
                CROSS APPLY fn_my_permissions(QUOTENAME(SCHEMA_NAME(o.schema_id)) + '.' + QUOTENAME(o.name), 'OBJECT') p
                WHERE o.type IN ('U', 'V')
                ORDER BY o.name, p.permission_name;"""

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

        logger.success(f"Retrieved {len(tables)} table(s) from [{target_database}]")

        return tables

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["[database | --database database | -db database]"]
