# mssqlclient_ng/core/services/user.py

# Built-in imports
from typing import Optional, Tuple

# Third party imports
from loguru import logger

# Local library imports
from .query import QueryService


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

        # Cache domain user status for each execution server
        self._is_domain_user_cache: dict[str, bool] = {}

        # Private user information
        self._mapped_user: Optional[str] = None
        self._system_user: Optional[str] = None
        self._effective_user: Optional[str] = None
        self._source_principal: Optional[str] = None

    @property
    def mapped_user(self) -> Optional[str]:
        """Get the mapped database user."""
        return self._mapped_user

    @mapped_user.setter
    def mapped_user(self, value: Optional[str]) -> None:
        """Set the mapped database user."""
        self._mapped_user = value

    @property
    def system_user(self) -> Optional[str]:
        """Get the system login user."""
        return self._system_user

    @system_user.setter
    def system_user(self, value: Optional[str]) -> None:
        """Set the system login user."""
        self._system_user = value

    @property
    def effective_user(self) -> Optional[str]:
        """Get the effective database user (handles AD group-based access)."""
        return self._effective_user

    @effective_user.setter
    def effective_user(self, value: Optional[str]) -> None:
        """Set the effective database user."""
        self._effective_user = value

    @property
    def source_principal(self) -> Optional[str]:
        """Get the source principal (AD group or login) that granted access."""
        return self._source_principal

    @source_principal.setter
    def source_principal(self, value: Optional[str]) -> None:
        """Set the source principal."""
        self._source_principal = value

    @property
    def is_domain_user(self) -> bool:
        r"""
        Check if the current system user is a Windows domain user.
        Uses username format (DOMAIN\username) as primary check.
        Results are cached per execution server.

        Returns:
            True if the user is a Windows domain user; otherwise False
        """
        execution_server = self._query_service.execution_server

        # Check cache first
        if execution_server in self._is_domain_user_cache:
            return self._is_domain_user_cache[execution_server]

        # Compute and cache the result
        domain_user_status = self._check_if_domain_user()
        self._is_domain_user_cache[execution_server] = domain_user_status

        return domain_user_status

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
            result = self._query_service.execute_scalar(
                f"SELECT IS_SRVROLEMEMBER('{role}');"
            )
            return int(result) == 1 if result is not None else False
        except Exception as e:
            logger.warning(f"Error checking role membership for role {role}: {e}")
            return False

    def _check_if_domain_user(self) -> bool:
        r"""
        Checks if the current system user is a Windows domain user.
        Uses username format (DOMAIN\username) as primary check.
        Linked server connections don't have sys.login_token, so format check is more reliable.

        Returns:
            True if the user is a Windows domain user; otherwise False
        """
        if not self._system_user:
            return False

        # Check if username has the DOMAIN\username format
        backslash_index = self._system_user.find("\\")
        if backslash_index <= 0 or backslash_index >= len(self._system_user) - 1:
            # No backslash or invalid format - not a domain user
            return False

        # Username has domain format - it's a Windows user
        return True

    def _get_effective_user_and_source(self) -> Tuple[str, str]:
        r"""
        Gets the effective database user and the source principal (AD group or login) that granted access.
        This handles cases where access is granted through AD group membership
        rather than direct login mapping (e.g., DOMAIN\User -> AD Group -> Database User).
        This uses the token from integrated Windows authentication.
        https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-login-token-transact-sql

        Returns:
            Tuple of (EffectiveUser, SourcePrincipal)
        """
        try:
            # If there's a direct mapping (MappedUser != SystemUser), use it
            if (
                self._mapped_user
                and self._system_user
                and self._mapped_user.lower() != self._system_user.lower()
            ):
                return (self._mapped_user, self._system_user)

            # Query user_token to find effective database user and login_token for source
            sql = """
SELECT TOP 1
    dp.name AS effective_user,
    lt.name AS source_principal
FROM sys.user_token ut
JOIN sys.database_principals dp ON dp.sid = ut.sid
LEFT JOIN sys.login_token lt ON lt.sid = ut.sid
WHERE ut.name <> 'public'
AND ut.type NOT IN ('ROLE', 'SERVER ROLE')
AND dp.principal_id > 0
ORDER BY dp.principal_id;"""

            rows = self._query_service.execute_table(sql)

            if not rows or len(rows) == 0:
                return (self._mapped_user or "Unknown", self._system_user or "Unknown")

            row = rows[0]
            effective = row.get("effective_user") or self._mapped_user or "Unknown"
            source = row.get("source_principal") or effective

            return (str(effective), str(source))
        except Exception as ex:
            logger.warning(f"Error determining effective user and source: {ex}")
            return (self._mapped_user or "Unknown", self._system_user or "Unknown")

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
                name = (
                    str(row.get("U", "Unknown"))
                    if row.get("U") is not None
                    else "Unknown"
                )
                logged_in_user_name = (
                    str(row.get("S", "Unknown"))
                    if row.get("S") is not None
                    else "Unknown"
                )
        except Exception as e:
            logger.warning(f"Error retrieving user info: {e}")

        # Use property setters
        self.mapped_user = name
        self.system_user = logged_in_user_name

        return (name, logged_in_user_name)

    def compute_effective_user_and_source(self) -> None:
        r"""
        Gets the effective database user and the source principal (AD group or login) that granted access.
        This handles cases where access is granted through AD group membership
        rather than direct login mapping (e.g., DOMAIN\User -> AD Group -> Database User).
        Uses the token from integrated Windows authentication.

        IMPORTANT: Only works on direct connections. Does NOT work through linked servers
        as sys.login_token is not available in remote execution contexts.

        https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-login-token-transact-sql
        """
        try:
            # If there's a direct mapping (MappedUser != SystemUser), use it
            if (
                self._mapped_user
                and self._system_user
                and self._mapped_user.lower() != self._system_user.lower()
            ):
                self.effective_user = self._mapped_user
                self.source_principal = self._system_user
                return

            # Query user_token to find effective database user and login_token for source
            sql = """
SELECT TOP 1
    dp.name AS effective_user,
    lt.name AS source_principal
FROM sys.user_token ut
JOIN sys.database_principals dp ON dp.sid = ut.sid
LEFT JOIN sys.login_token lt ON lt.sid = ut.sid
WHERE ut.name <> 'public'
AND ut.type NOT IN ('ROLE', 'SERVER ROLE')
AND dp.principal_id > 0
ORDER BY dp.principal_id;"""

            rows = self._query_service.execute_table(sql)

            if not rows or len(rows) == 0:
                self.effective_user = self._mapped_user or "Unknown"
                self.source_principal = self._system_user or "Unknown"
                return

            row = rows[0]
            self.effective_user = (
                row.get("effective_user") or self._mapped_user or "Unknown"
            )
            self.source_principal = row.get("source_principal") or self.effective_user
        except Exception as ex:
            logger.warning(f"Error determining effective user and source: {ex}")
            self.effective_user = self._mapped_user or "Unknown"
            self.source_principal = self._system_user or "Unknown"

    def get_user_database_roles(self) -> list[str]:
        """
        Retrieves the list of database roles the current user is a member of.
        Checks roles in the current database context.

        Returns:
            List of database role names the user belongs to, or empty list if none found
        """
        roles = []

        try:
            # Get all database roles that the current user is a member of
            roles_query = """
                SELECT r.name
                FROM sys.database_principals r
                INNER JOIN sys.database_role_members rm ON r.principal_id = rm.role_principal_id
                INNER JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id
                WHERE m.name = USER_NAME()
                AND r.type = 'R'
                ORDER BY r.name;"""

            roles_table = self._query_service.execute_table(roles_query)

            for row in roles_table:
                role_name = row.get("name")
                if role_name:
                    roles.append(str(role_name))
        except Exception as ex:
            logger.warning(f"Error retrieving database roles: {ex}")

        return roles

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
            logger.info(
                f"You can impersonate anyone on {self._query_service.execution_server} as a sysadmin"
            )
            return True

        query = (
            "SELECT 1 FROM master.sys.server_permissions a "
            "INNER JOIN master.sys.server_principals b ON a.grantor_principal_id = b.principal_id "
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
