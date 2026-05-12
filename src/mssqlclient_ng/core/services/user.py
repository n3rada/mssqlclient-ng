# mssqlclient_ng/core/services/user.py

# Built-in imports
from typing import List, Optional, Tuple

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

        # Cache server-level permission checks, keyed by "hostname:permission"
        self._permission_cache: dict[str, bool] = {}

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

        # Quick check: sa login is always sysadmin
        if self._system_user and self._system_user.lower() == "sa":
            self._admin_status_cache[execution_server] = True
            return True

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
                f"SELECT IS_SRVROLEMEMBER('{role}');", silent=True
            )
            return int(result) == 1 if result is not None else False
        except Exception as e:
            logger.debug(f"Error checking role membership for role {role}: {e}")
            return False

    def has_permission(self, permission: str) -> bool:
        """
        Checks whether the current login holds a specific server-level permission.
        Results are cached per execution server to avoid redundant round-trips.

        Args:
            permission: Server-level permission name (e.g. "CONTROL SERVER", "ALTER ANY LOGIN")

        Returns:
            True if the current login has the permission; otherwise False
        """
        cache_key = f"{self._query_service.execution_server}:{permission}"
        if cache_key in self._permission_cache:
            return self._permission_cache[cache_key]

        result = False
        try:
            safe_perm = permission.replace("'", "''")
            val = self._query_service.execute_scalar(
                f"SELECT HAS_PERMS_BY_NAME(NULL, NULL, '{safe_perm}')"
            )
            result = int(val) == 1 if val is not None else False
        except Exception as e:
            logger.warning(f"Error checking permission '{permission}': {e}")

        self._permission_cache[cache_key] = result
        return result

    @staticmethod
    def is_system_account(login: str) -> bool:
        """
        Returns True if the login is a Windows system account (NT AUTHORITY\\, NT SERVICE\\, etc.).
        These accounts add no unique linked server mapping information.

        Args:
            login: The login name to check

        Returns:
            True if it's a system account; otherwise False
        """
        if not login:
            return False
        upper = login.upper()
        return upper.startswith("NT ") or upper.startswith("NT\\")

    def get_server_roles(self) -> Tuple[List[str], List[str]]:
        """
        Returns the current user's server role memberships split into fixed and custom roles.
        Excludes the public role and internal placeholder roles (##...##).
        Also populates the admin-status cache as a side effect.

        Returns:
            Tuple of (fixed_roles, custom_roles)
        """
        query = """
SELECT name, is_fixed_role
FROM sys.server_principals
WHERE type = 'R'
  AND name != 'public'
  AND name NOT LIKE '##%##'
  AND ISNULL(IS_SRVROLEMEMBER(name), 0) = 1
ORDER BY is_fixed_role DESC, name;"""

        fixed_roles: List[str] = []
        custom_roles: List[str] = []
        try:
            rows = self._query_service.execute_table(query, silent=True)
            for row in rows:
                name = str(row.get("name", ""))
                is_fixed = row.get("is_fixed_role")
                if is_fixed and (is_fixed == 1 or str(is_fixed).lower() == "true"):
                    fixed_roles.append(name)
                else:
                    custom_roles.append(name)

            # Side effect: cache sysadmin status
            is_sysadmin = any(r.lower() == "sysadmin" for r in fixed_roles)
            self._admin_status_cache[self._query_service.execution_server] = is_sysadmin
        except Exception:
            pass

        return (fixed_roles, custom_roles)

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

    def get_info(self) -> Tuple[str, str]:
        """
        Retrieve information about the current user.

        Returns:
            Tuple containing (mapped_user, system_user)
        """
        query = "SELECT USER_NAME() AS U, SYSTEM_USER AS S;"

        name = ""
        logged_in_user_name = ""

        rows = self._query_service.execute(query, tuple_mode=False, silent=True)

        if rows and len(rows) > 0:
            row = rows[0]
            u = row.get("U")
            s = row.get("S")
            if u is not None:
                name = str(u)
            if s is not None:
                logged_in_user_name = str(s)

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
            self.effective_user = self._mapped_user

            # Check if SYSTEM_USER has a direct Windows login (type 'U') in sys.server_principals.
            # This is a single indexed lookup — cheap for the common case.
            login_type = self._query_service.execute_scalar(
                "SELECT type FROM sys.server_principals WHERE name = SYSTEM_USER;"
            )

            if login_type and str(login_type) == "U":
                self.source_principal = self._system_user
                return

            # No direct login — access granted via an AD group.
            # Find the group in sys.login_token joined to sys.server_principals.
            group = self._query_service.execute_scalar("""
SELECT TOP 1 sp.name
FROM sys.login_token lt
INNER JOIN sys.server_principals sp ON sp.sid = lt.sid
WHERE lt.type = 'WINDOWS GROUP' AND sp.type = 'G'
ORDER BY sp.principal_id;""")

            self.source_principal = str(group) if group else self._system_user
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
        For direct connections: issues EXECUTE AS LOGIN that persists in the session.
        For linked servers: updates the impersonation arrays prepended to every query
        (EXECUTE AS doesn't persist across separate EXEC() AT calls).

        Args:
            user: The login to impersonate

        Returns:
            True if impersonation was successful; otherwise False
        """
        # For linked servers, push to the impersonation chain (like C#'s PushLinkedImpersonation)
        if not self._query_service.linked_servers.is_empty:
            self._push_linked_impersonation(user)
            self._admin_status_cache.clear()
            return True

        safe_user = user.replace("'", "''")
        query = f"EXECUTE AS LOGIN = N'{safe_user}';"

        try:
            self._query_service.execute_non_processing(query, silent=True)
            self._admin_status_cache.clear()
            logger.debug(f"Impersonated user {user} for current connection")
            return True
        except Exception as e:
            error_msg = str(e)
            # Handle error 916: cannot access database after impersonation attempt
            if "is not able to access the database" in error_msg:
                logger.debug(
                    f"Switching to master before impersonating '{user}' (current DB inaccessible)"
                )
                try:
                    self._query_service.execute_non_processing(
                        "USE master;", silent=True
                    )
                    self._query_service.execute_non_processing(query, silent=True)
                    self._admin_status_cache.clear()
                    logger.debug(
                        f"Impersonated user {user} for current connection (via master)"
                    )
                    return True
                except Exception as retry_ex:
                    logger.error(f"Failed to impersonate user {user}: {retry_ex}")
                    return False
            logger.error(f"Failed to impersonate user {user}: {e}")
            return False

    def revert_impersonation(self) -> bool:
        """
        Revert any active impersonation and restore the original login.
        For direct connections: issues a REVERT command.
        For linked servers: pops the last login from the impersonation arrays.

        Returns:
            True if revert was successful; otherwise False
        """
        # For linked servers, pop from the impersonation chain (like C#'s PopLinkedImpersonation)
        if not self._query_service.linked_servers.is_empty:
            self._pop_linked_impersonation()
            self._admin_status_cache.clear()
            return True

        query = "REVERT;"

        try:
            self._query_service.execute_non_processing(query)
            self._admin_status_cache.clear()
            logger.debug("Reverted impersonation")
            return True
        except Exception as e:
            logger.error(f"Failed to revert impersonation: {e}")
            return False

    def _push_linked_impersonation(self, login: str) -> None:
        """
        Pushes a login onto the linked server impersonation chain (last server in the chain).
        This modifies the LinkedServers model so that subsequent queries include EXECUTE AS.
        """
        chain = self._query_service.linked_servers.server_chain
        if not chain:
            return
        last_server = chain[-1]
        last_server.impersonation_users = list(last_server.impersonation_users) + [
            login
        ]
        self._query_service.linked_servers._recompute_chain()
        logger.debug(
            f"Linked impersonation chain: {' -> '.join(last_server.impersonation_users)}"
        )

    def _pop_linked_impersonation(self) -> None:
        """
        Pops the last login from the linked server impersonation chain (last server in the chain).
        """
        chain = self._query_service.linked_servers.server_chain
        if not chain:
            return
        last_server = chain[-1]
        users = list(last_server.impersonation_users)
        if users:
            users.pop()
        last_server.impersonation_users = users
        self._query_service.linked_servers._recompute_chain()
        logger.debug(f"Linked impersonation reverted to: {users if users else 'none'}")

    def clear_admin_cache(self) -> None:
        """Clear the admin status cache for all servers."""
        self._admin_status_cache.clear()
        logger.debug("Admin status cache cleared")

    def clear_caches(self) -> None:
        """
        Clear all cached admin, domain user, and permission status.
        Call this when the execution context changes (e.g., after modifying the linked server chain).
        """
        self._admin_status_cache.clear()
        self._is_domain_user_cache.clear()
        self._permission_cache.clear()
        logger.debug("All user service caches cleared")

    def clear_admin_cache_for_server(self, server: str) -> None:
        """
        Clear the admin status cache for a specific server.

        Args:
            server: The server name to clear from cache
        """
        if server in self._admin_status_cache:
            del self._admin_status_cache[server]
            logger.debug(f"Admin status cache cleared for server: {server}")
