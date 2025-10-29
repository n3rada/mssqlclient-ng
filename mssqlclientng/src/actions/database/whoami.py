# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from mssqlclientng.src.actions.base import BaseAction
from mssqlclientng.src.actions.factory import ActionFactory
from mssqlclientng.src.services.database import DatabaseContext
from mssqlclientng.src.utils import formatter


@ActionFactory.register("whoami", "Display current user identity and permissions")
class Whoami(BaseAction):
    """
    Displays detailed information about the current user.

    Shows user identity, assigned roles, accessible databases, and fixed server role memberships.
    """

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        No additional arguments needed for whoami.

        Args:
            additional_arguments: Ignored.
        """
        pass

    def execute(self, database_context: DatabaseContext) -> Optional[dict]:
        """
        Executes the whoami action.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            Dictionary with user information.
        """
        user_name, system_user = database_context.user_service.get_info()

        # Fetch roles assigned to the current user (checks both direct and inherited via IS_SRVROLEMEMBER)
        # This single query checks all fixed server roles
        roles_query = """
            SELECT
                IS_SRVROLEMEMBER('sysadmin') AS sysadmin,
                IS_SRVROLEMEMBER('serveradmin') AS serveradmin,
                IS_SRVROLEMEMBER('setupadmin') AS setupadmin,
                IS_SRVROLEMEMBER('processadmin') AS processadmin,
                IS_SRVROLEMEMBER('diskadmin') AS diskadmin,
                IS_SRVROLEMEMBER('dbcreator') AS dbcreator,
                IS_SRVROLEMEMBER('bulkadmin') AS bulkadmin;
        """
        result = database_context.query_service.execute_table(roles_query)

        # Define fixed server roles with descriptions
        fixed_server_roles = [
            ("sysadmin", "Full control over the SQL Server instance"),
            ("serveradmin", "Manage server-wide configurations"),
            ("setupadmin", "Manage linked servers and setup tasks"),
            ("processadmin", "Terminate and monitor processes"),
            ("diskadmin", "Manage disk files for databases"),
            ("dbcreator", "Create and alter databases"),
            ("bulkadmin", "Perform bulk data imports"),
        ]

        # Extract roles user has from the single query result
        user_roles = set()
        if result:
            for role, _ in fixed_server_roles:
                if result[0].get(role) == 1:
                    user_roles.add(role)

        # Query for accessible databases
        accessible_databases = database_context.query_service.execute_table(
            "SELECT name FROM sys.databases WHERE HAS_DBACCESS(name) = 1;"
        )

        database_names = []
        if accessible_databases:
            database_names = [db["name"] for db in accessible_databases]

        # Display the user information
        logger.info("User Details")
        user_info = {
            "User Name": user_name,
            "System User": system_user,
            "Roles": ", ".join(sorted(user_roles)) if user_roles else "None",
            "Accessible Databases": (
                ", ".join(database_names) if database_names else "None"
            ),
        }
        print(formatter.dict_to_markdown_table(user_info, "Property", "Value"))

        # Create list to display fixed server roles with membership status
        fixed_server_roles_list = []
        for role, responsibility in fixed_server_roles:
            fixed_server_roles_list.append(
                {
                    "Role": role,
                    "Key Responsibility": responsibility,
                    "Has": role in user_roles,
                }
            )

        # Display the fixed server roles table
        logger.info("Fixed Server Roles")
        print(formatter.rows_to_markdown_table(fixed_server_roles_list))

        return {
            "user_name": user_name,
            "system_user": system_user,
            "roles": list(user_roles),
            "accessible_databases": database_names,
        }

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            Empty list as no arguments are required.
        """
        return []
