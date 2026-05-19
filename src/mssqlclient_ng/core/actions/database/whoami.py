# Built-in imports

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter

@ActionFactory.register(
    "whoami", "Display current user identity and permissions", aliases=["id", "groups"]
)
class Whoami(BaseAction):
    """
    Displays detailed information about the current user.

    Shows user identity, server roles (fixed and custom), database roles, and accessible databases.
    """

    def execute(self, database_context: DatabaseContext) -> dict | None:
        """
        Executes the whoami action.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            None (displays user information).
        """
        logger.info("Retrieving current user information")

        user_name, system_user = database_context.user_service.get_info()

        fixed_roles, custom_roles = database_context.user_service.get_server_roles()

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

        user_details = {
            "Login": system_user,
            "Mapped to user": user_name,
            "Server Fixed Roles": ", ".join(fixed_roles),
            "Server Custom Roles": ", ".join(custom_roles),
            "Database Roles": ", ".join(user_db_roles) if user_db_roles else "",
            "Accessible Databases": ", ".join(database_names) if database_names else "",
        }

        print(OutputFormatter.convert_dict(user_details, "Property", "Value"))

        return None
