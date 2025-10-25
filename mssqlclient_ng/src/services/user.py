"""
User service for managing SQL Server user information and permissions.
"""
from typing import Optional, Tuple
from loguru import logger

from mssqlclient_ng.src.services.query import QueryService


class UserService:
    """
    Service for managing user information, role membership, and impersonation.
    """

    def __init__(self, query_service: QueryService):
        """
        Initialize the user service.

        Args:
            query_service: The query service instance to use for database operations
        """
        self._query_service = query_service

        # Cache admin status for each execution server
        self._admin_status_cache: dict[str, bool] = {}

        # User information
        self.mapped_user: Optional[str] = None
        self.system_user: Optional[str] = None

    def is_admin(self) -> bool:
        """
        Check if the current user has sysadmin privileges.
        Results are cached per execution server.

        Returns:
            True if the user is a sysadmin; otherwise False
        """
        execution_server = self._query_service.execution_server

        # Check cache first
        if execution_server in self._admin_status_cache:
            return self._admin_status_cache[execution_server]

        # Compute and cache the result
        admin_status = self.is_member_of_role("sysadmin")
        self._admin_status_cache[execution_server] = admin_status

        return admin_status

    def is_member_of_role(self, role: str) -> bool:
        """
        Check if the current user is a member of a specified server role.

        Args:
            role: The role to check (e.g., 'sysadmin', 'dbcreator', 'securityadmin')

        Returns:
            True if the user is a member of the role; otherwise False
        """
        try:
            result = self._query_service.execute_scalar(f"SELECT IS_SRVROLEMEMBER('{role}');")
            return int(result) == 1 if result is not None else False
        except Exception as e:
            logger.warning(f"Error checking role membership for role {role}: {e}")
            return False

    def get_info(self) -> Tuple[str, str]:
        """
        Retrieve information about the current user.

        Returns:
            Tuple containing (mapped_user, system_user)
        """
        query = "SELECT USER_NAME() AS U, SYSTEM_USER AS S;"

        name = "Unknown"
        logged_in_user_name = "Unknown"

        try:
            rows = self._query_service.execute(query, tuple_mode=False)

            if rows and len(rows) > 0:
                row = rows[0]
                name = str(row.get("U", "Unknown")) if row.get("U") is not None else "Unknown"
                logged_in_user_name = str(row.get("S", "Unknown")) if row.get("S") is not None else "Unknown"
        except Exception as e:
            logger.warning(f"Error retrieving user info: {e}")

        self.mapped_user = name
        self.system_user = logged_in_user_name

        return (name, logged_in_user_name)

    def can_impersonate(self, user: str) -> bool:
        """
        Check if the current user can impersonate a specified login.

        Args:
            user: The login to check for impersonation

        Returns:
            True if the user can impersonate the specified login; otherwise False
        """
        # A sysadmin user can impersonate anyone
        if self.is_admin():
            logger.info(f"You can impersonate anyone on {self._query_service.execution_server} as a sysadmin")
            return True

        query = (
            "SELECT 1 FROM sys.server_permissions a "
            "INNER JOIN sys.server_principals b ON a.grantor_principal_id = b.principal_id "
            f"WHERE a.permission_name = 'IMPERSONATE' AND b.name = '{user}';"
        )

        try:
            result = self._query_service.execute_scalar(query)
            return int(result) == 1 if result is not None else False
        except Exception as e:
            logger.warning(f"Error checking impersonation for user {user}: {e}")
            return False

    def impersonate_user(self, user: str) -> bool:
        """
        Impersonate a specified user on the current connection.

        Args:
            user: The login to impersonate

        Returns:
            True if impersonation was successful; otherwise False
        """
        query = f"EXECUTE AS LOGIN = '{user}';"

        try:
            self._query_service.execute_non_processing(query)
            logger.info(f"Impersonated user {user} for current connection")
            return True
        except Exception as e:
            logger.error(f"Failed to impersonate user {user}: {e}")
            return False

    def revert_impersonation(self) -> bool:
        """
        Revert any active impersonation and restore the original login.

        Returns:
            True if revert was successful; otherwise False
        """
        query = "REVERT;"

        try:
            self._query_service.execute_non_processing(query)
            logger.info("Reverted impersonation, restored original login.")
            return True
        except Exception as e:
            logger.error(f"Failed to revert impersonation: {e}")
            return False

    def clear_admin_cache(self) -> None:
        """Clear the admin status cache for all servers."""
        self._admin_status_cache.clear()
        logger.debug("Admin status cache cleared")

    def clear_admin_cache_for_server(self, server: str) -> None:
        """
        Clear the admin status cache for a specific server.

        Args:
            server: The server name to clear from cache
        """
        if server in self._admin_status_cache:
            del self._admin_status_cache[server]
            logger.debug(f"Admin status cache cleared for server: {server}")
