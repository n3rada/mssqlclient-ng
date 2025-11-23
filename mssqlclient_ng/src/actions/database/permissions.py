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
        self._database: str = ""
        self._schema: Optional[str] = None  # Let SQL Server use user's default schema
        self._table: str = ""

    def _sort_permissions_by_importance(self, permissions: list[dict]) -> list[dict]:
        """
        Sorts permissions by exploitation value - most interesting permissions first.

        Args:
            permissions: List of permission dictionaries

        Returns:
            Sorted list of permissions
        """
        # Define permission priority order (lower number = higher priority)
        permission_priority = {
            # Most powerful permissions
            "CONTROL": 0,
            "CONTROL SERVER": 0,
            "ALTER": 1,
            "ALTER ANY DATABASE": 1,
            "IMPERSONATE": 2,
            "IMPERSONATE ANY LOGIN": 2,
            "TAKE OWNERSHIP": 3,
            # Execute permissions
            "EXECUTE": 10,
            "EXECUTE ANY EXTERNAL SCRIPT": 10,
            # Data modification
            "INSERT": 20,
            "UPDATE": 21,
            "DELETE": 22,
            # Read permissions
            "SELECT": 30,
            "VIEW DEFINITION": 31,
            "VIEW SERVER STATE": 32,
            "VIEW DATABASE STATE": 32,
            # Other permissions
            "REFERENCES": 40,
            "CONNECT": 50,
            "CONNECT SQL": 50,
        }

        def get_priority(perm_dict):
            perm_name = perm_dict.get("Permission", "")
            # Get priority from dict, default to 100 for unknown permissions
            priority = permission_priority.get(perm_name, 100)
            return (priority, perm_name)

        return sorted(permissions, key=get_priority)

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the arguments for the permissions action.

        Args:
            additional_arguments: Empty for server/database permissions,
                                 schema.table for current database, or
                                 database.schema.table for specific database

        Raises:
            ValueError: If the format is invalid.
        """
        if not additional_arguments or not additional_arguments.strip():
            # No arguments - will show server and database permissions
            return

        self._fqtn = additional_arguments.strip()
        parts = self._fqtn.split(".")

        if len(parts) == 3:  # Format: database.schema.table
            self._database = parts[0]
            self._schema = parts[1]  # Use explicitly specified schema
            self._table = parts[2]
        elif len(parts) == 2:  # Format: schema.table (current database)
            self._database = ""  # Use current database
            self._schema = parts[0]  # Use explicitly specified schema
            self._table = parts[1]
        else:
            raise ValueError(
                "Invalid format. Expected 'database.schema.table', 'schema.table', or nothing for server permissions."
            )

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        """
        Executes the permissions enumeration.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            None (prints results directly).
        """
        if not self._table:
            # Show server and database permissions
            logger.info(
                "Listing permissions of the current user on server and accessible databases"
            )

            logger.info("Server permissions")
            server_perms = database_context.query_service.execute_table(
                "SELECT permission_name AS Permission FROM fn_my_permissions(NULL, 'SERVER');"
            )
            sorted_server_perms = self._sort_permissions_by_importance(server_perms)
            print(OutputFormatter.convert_list_of_dicts(sorted_server_perms))

            logger.info("Database permissions")
            db_perms = database_context.query_service.execute_table(
                "SELECT permission_name AS Permission FROM fn_my_permissions(NULL, 'DATABASE');"
            )
            sorted_db_perms = self._sort_permissions_by_importance(db_perms)
            print(OutputFormatter.convert_list_of_dicts(sorted_db_perms))

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

        query = f"""
            USE [{self._database}];
            SELECT DISTINCT
                permission_name AS [Permission]
            FROM
                fn_my_permissions('{target_table}', 'OBJECT');
        """

        try:
            result = database_context.query_service.execute_table(query)

            if not result:
                logger.warning("No permissions found or table does not exist")
                return []

            sorted_result = self._sort_permissions_by_importance(result)
            print(OutputFormatter.convert_list_of_dicts(sorted_result))

            return sorted_result

        except Exception as e:
            logger.error(f"Failed to retrieve permissions: {e}")
            raise

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List containing the optional table name argument.
        """
        return ["[database.schema.table or schema.table]"]
