# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register("whoami", "Display current user identity and permissions")
class Whoami(BaseAction):
    """
    Displays detailed information about the current user.

    Shows user identity, server roles (fixed and custom), database roles, and accessible databases.
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
            None (displays user information).
        """
        logger.info("Retrieving current user information")

        user_name, system_user = database_context.user_service.get_info()

        # Get all roles and check membership in a single query
        # This uses IS_SRVROLEMEMBER which works even with AD group-based access
        roles_query = """
            SELECT
                name,
                is_fixed_role,
                ISNULL(IS_SRVROLEMEMBER(name), 0) AS is_member
            FROM sys.server_principals
            WHERE type = 'R'
            ORDER BY is_fixed_role DESC, name;
        """

        all_roles_table = database_context.query_service.execute_table(roles_query)

        fixed_roles = []
        custom_roles = []
        user_roles = set()

        # Separate roles and collect user memberships
        if all_roles_table:
            for role_row in all_roles_table:
                role_name = role_row["name"]
                is_fixed_role = bool(role_row["is_fixed_role"])
                is_member = role_row["is_member"] == 1

                if is_fixed_role:
                    fixed_roles.append((role_name, is_member))
                else:
                    custom_roles.append((role_name, is_member))

                if is_member:
                    user_roles.add(role_name)

        # Query for accessible databases
        accessible_databases = database_context.query_service.execute_table(
            "SELECT name FROM master.sys.databases WHERE HAS_DBACCESS(name) = 1;"
        )

        database_names = []
        if accessible_databases:
            database_names = [db["name"] for db in accessible_databases]

        # Get database roles in current database
        db_roles_query = """
            SELECT
                name,
                ISNULL(IS_ROLEMEMBER(name), 0) AS is_member
            FROM sys.database_principals
            WHERE type = 'R'
            ORDER BY name;
        """

        db_roles_table = database_context.query_service.execute_table(db_roles_query)

        user_db_roles = []
        if db_roles_table:
            for db_role_row in db_roles_table:
                if db_role_row["is_member"] == 1:
                    user_db_roles.append(db_role_row["name"])

        # Display the user information
        logger.info("User Details:")

        # Only show roles where user is a member
        user_fixed_roles = [r[0] for r in fixed_roles if r[1]]
        user_custom_roles = [r[0] for r in custom_roles if r[1]]

        user_details = {
            "User Name": user_name,
            "System User": system_user,
            "Server Fixed Roles": (
                ", ".join(user_fixed_roles) if user_fixed_roles else ""
            ),
            "Server Custom Roles": (
                ", ".join(user_custom_roles) if user_custom_roles else ""
            ),
            "Database Roles": ", ".join(user_db_roles) if user_db_roles else "",
            "Accessible Databases": ", ".join(database_names) if database_names else "",
        }

        print(OutputFormatter.convert_dict(user_details, "Property", "Value"))

        return None

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            Empty list as no arguments are required.
        """
        return []
