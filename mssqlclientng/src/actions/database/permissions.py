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
    "permissions",
    "List permissions for the current user on server, databases, or specific table",
)
class Permissions(BaseAction):
    """
    Lists permissions for the current user.

    Without arguments: Shows server permissions, database permissions, and accessible databases.
    With FQTN (database.schema.table or database..table): Shows permissions on the specified table.
    """

    def __init__(self):
        super().__init__()
        self._fqtn: str = ""
        self._database: str = ""
        self._schema: str = "dbo"  # Default schema
        self._table: str = ""

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the arguments for the permissions action.

        Args:
            additional_arguments: Empty for server/database permissions, or
                                 fully qualified table name (database.schema.table or database..table)

        Raises:
            ValueError: If the FQTN format is invalid.
        """
        if not additional_arguments or not additional_arguments.strip():
            # No arguments - will show server and database permissions
            return

        self._fqtn = additional_arguments.strip()
        parts = self._fqtn.split(".")

        if len(parts) == 3:  # Format: database.schema.table
            self._database = parts[0]

            if parts[1]:
                self._schema = parts[1]
            else:
                self._schema = "dbo"  # Default if empty (e.g., database..table)

            self._table = parts[2]
        else:
            raise ValueError(
                "Invalid format for the argument. Expected 'database.schema.table' or "
                "'database..table' or nothing to return current server permissions."
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
            print(formatter.rows_to_markdown_table(server_perms))

            logger.info("Database permissions")
            db_perms = database_context.query_service.execute_table(
                "SELECT permission_name AS Permission FROM fn_my_permissions(NULL, 'DATABASE');"
            )
            print(formatter.rows_to_markdown_table(db_perms))

            logger.info("Database access")
            accessible_dbs = database_context.query_service.execute_table(
                "SELECT name AS [Accessible Database] FROM sys.databases WHERE HAS_DBACCESS(name) = 1;"
            )
            print(formatter.rows_to_markdown_table(accessible_dbs))

            return None

        # Show table-specific permissions
        target_table = f"[{self._schema}].[{self._table}]"
        mapped_user = database_context.user_service.mapped_user

        logger.info(
            f"Listing permissions for {mapped_user} on [{self._database}].{target_table}"
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

            print(formatter.rows_to_markdown_table(result))

            return result

        except Exception as e:
            logger.error(f"Failed to retrieve permissions: {e}")
            raise

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List containing the optional FQTN argument.
        """
        return ["[database.schema.table]"]
