# mssqlclient_ng/core/actions/database/permissions.py

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
    "permissions",
    "List permissions for the current user on server, databases, or specific table",
)
class Permissions(BaseAction):
    """
    Enumerate user and role permissions at server, database, and object levels.

    Usage:
    - No arguments: Show current user's server, database, and database access permissions
    - schema.table: Show permissions on a specific table in the current database
    - database.schema.table: Show permissions on a specific table in a specific database

    Uses fn_my_permissions to check what the current user can do.
    Schema defaults to the user's default schema if not explicitly specified.
    """

    def __init__(self):
        super().__init__()
        self._fqtn: str = ""
        self._database: Optional[str] = None
        self._schema: Optional[str] = None  # Let SQL Server use user's default schema
        self._table: str = ""

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate the arguments for the permissions action.

        Args:
            additional_arguments: Argument string to parse

        Raises:
            ValueError: If the format is invalid
        """
        if not additional_arguments or not additional_arguments.strip():
            # No arguments - will show server and database permissions
            return

        # Parse both positional and named arguments
        named_args, positional_args = self._parse_action_arguments(additional_arguments)

        # Get table name from position 0
        table_name = positional_args[0] if len(positional_args) >= 1 else ""

        if not table_name:
            raise ValueError(
                "Invalid format for the argument. Expected 'database.schema.table', "
                "'schema.table', or nothing to return current server permissions."
            )

        self._fqtn = table_name

        # Parse the table name to extract database, schema, and table
        parts = table_name.split(".")

        if len(parts) == 3:  # Format: database.schema.table
            self._database = parts[0]
            self._schema = parts[1]  # Use explicitly specified schema
            self._table = parts[2]
        elif len(parts) == 2:  # Format: schema.table (current database)
            self._database = None  # Use current database
            self._schema = parts[0]  # Use explicitly specified schema
            self._table = parts[1]
        else:
            raise ValueError(
                "Invalid format for the argument. Expected 'database.schema.table', "
                "'schema.table', or nothing to return current server permissions."
            )

    def execute(self, database_context: DatabaseContext) -> None:
        """
        Execute the permissions enumeration.

        Args:
            database_context: The DatabaseContext instance to execute the query

        Returns:
            None
        """
        if not self._table:
            logger.info(
                "Listing permissions of the current user on server and accessible databases"
            )
            print()
            logger.info("Server permissions")

            server_perms = database_context.query_service.execute_table(
                "SELECT permission_name AS Permission FROM fn_my_permissions(NULL, 'SERVER');"
            )
            sorted_server_perms = self._sort_permissions_by_importance(server_perms)
            print(OutputFormatter.convert_list_of_dicts(sorted_server_perms))
            print()

            logger.info("Database permissions")

            db_perms = database_context.query_service.execute_table(
                "SELECT permission_name AS Permission FROM fn_my_permissions(NULL, 'DATABASE');"
            )
            sorted_db_perms = self._sort_permissions_by_importance(db_perms)
            print(OutputFormatter.convert_list_of_dicts(sorted_db_perms))
            print()

            logger.info("Database access")

            accessible_dbs = database_context.query_service.execute_table(
                "SELECT name AS [Accessible Database] FROM master.sys.databases WHERE HAS_DBACCESS(name) = 1;"
            )
            print(OutputFormatter.convert_list_of_dicts(accessible_dbs))

            return None

        # Use the execution database if no database is specified
        if not self._database:
            self._database = database_context.query_service.execution_database

        # Build the target table name based on what was specified
        if self._schema:
            target_table = f"[{self._schema}].[{self._table}]"
        else:
            # No schema specified - let SQL Server use the user's default schema
            target_table = f"..[{self._table}]"

        mapped_user = database_context.user_service.mapped_user

        logger.info(
            f"Listing permissions for {mapped_user} on [{self._database}]{target_table}"
        )

        # Build USE statement if specific database is different from current
        use_statement = (
            ""
            if not self._database
            or self._database == database_context.query_service.execution_database
            else f"USE [{self._database}];"
        )

        # Query to get permissions
        query = f"""
            {use_statement}
            SELECT DISTINCT
                permission_name AS [Permission]
            FROM
                fn_my_permissions('{target_table}', 'OBJECT');
            """

        data_table = database_context.query_service.execute_table(query)
        sorted_table = self._sort_permissions_by_importance(data_table)

        print(OutputFormatter.convert_list_of_dicts(sorted_table))
        return None

    def _sort_permissions_by_importance(self, permissions: List[Dict]) -> List[Dict]:
        """
        Sort permissions by exploitation value - most interesting permissions first.
        Secondary sort by permission name for consistent ordering.

        Args:
            permissions: List of permission dictionaries

        Returns:
            Sorted list of permissions
        """
        if not permissions:
            return permissions

        def get_sort_key(perm_dict):
            # Sort by priority first, then by permission name alphabetically
            perm_name = perm_dict.get("Permission", "")
            priority = self._get_permission_priority(perm_name)
            return (priority, perm_name)

        return sorted(permissions, key=get_sort_key)

    def _get_permission_priority(self, permission: str) -> int:
        """
        Return a priority value for a permission. Lower values = higher importance/exploitation value.

        Args:
            permission: Permission name

        Returns:
            Priority value (lower is more important)
        """
        # Critical server-level permissions (most dangerous)
        if permission == "CONTROL SERVER":
            return 1
        if permission == "ALTER ANY LOGIN":
            return 2
        if permission == "ALTER ANY DATABASE":
            return 3
        if permission == "CREATE ANY DATABASE":
            return 4

        # Administrative permissions
        if permission == "CONTROL":
            return 10
        if permission == "TAKE OWNERSHIP":
            return 11
        if permission == "IMPERSONATE":
            return 12
        if permission == "ALTER ANY USER":
            return 13
        if permission == "ALTER ANY ROLE":
            return 14
        if permission == "ALTER ANY SCHEMA":
            return 15

        # Code execution permissions
        if permission == "EXECUTE":
            return 20
        if permission == "ALTER":
            return 21
        if permission == "CREATE PROCEDURE":
            return 22
        if permission == "CREATE FUNCTION":
            return 23
        if permission == "CREATE ASSEMBLY":
            return 24

        # Data modification permissions
        if permission == "INSERT":
            return 30
        if permission == "UPDATE":
            return 31
        if permission == "DELETE":
            return 32

        # Data access permissions
        if permission == "SELECT":
            return 40
        if permission == "REFERENCES":
            return 41

        # View/metadata permissions
        if permission == "VIEW DEFINITION":
            return 50
        if permission == "VIEW ANY DATABASE":
            return 51
        if permission == "VIEW SERVER STATE":
            return 52
        if permission == "VIEW DATABASE STATE":
            return 53

        # Connection permissions (least critical)
        if permission == "CONNECT":
            return 60
        if permission == "CONNECT SQL":
            return 61

        # Default for unknown permissions
        return 100

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["[database.schema.table or schema.table]"]
