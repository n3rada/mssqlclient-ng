# mssqlclient_ng/core/actions/database/impersonate.py

# Built-in imports
from typing import Optional, List, Dict, Any

# Third-party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "impersonate",
    "Check which SQL logins can be impersonated by current user",
    aliases=["impersonation", "imp"],
)
class Impersonation(BaseAction):
    """
    Check SQL Server impersonation permissions.

    Lists all SQL logins and Windows principals, and checks which ones
    can be impersonated by the current user. Sysadmin users can impersonate
    any login.
    """

    def validate_arguments(
        self, additional_arguments: str = "", argument_list: Optional[List[str]] = None
    ) -> None:
        """
        Validate arguments (none required for this action).

        Args:
            additional_arguments: Not used
            argument_list: Not used
        """
        # No arguments needed
        pass

    def execute(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Check impersonation permissions for SQL logins and Windows principals.

        Args:
            database_context: The database context

        Returns:
            List of users with their impersonation status
        """
        logger.info("Starting impersonation check")

        try:
            # Query to obtain all SQL logins and Windows principals with impersonation check
            query = """
            SELECT
                sp.name,
                sp.type_desc,
                sp.create_date,
                sp.modify_date,
                HAS_PERMS_BY_NAME(sp.name, 'LOGIN', 'IMPERSONATE') AS can_impersonate
            FROM sys.server_principals sp
            WHERE sp.type_desc IN ('SQL_LOGIN', 'WINDOWS_LOGIN', 'WINDOWS_GROUP')
              AND sp.name NOT LIKE '##%'
              AND sp.name != SYSTEM_USER
            ORDER BY can_impersonate DESC, sp.create_date DESC;
            """

            result_rows = database_context.query_service.execute_table(query)

            if not result_rows:
                logger.warning("No SQL logins or Windows principals found")
                return result_rows

            # Check if the current user is a sysadmin
            is_sysadmin = database_context.user_service.is_admin()

            if is_sysadmin:
                logger.success(
                    "Current user is 'sysadmin'; it can impersonate any login"
                )
                # Impersonation column is redundant when sysadmin
                enriched_users = []
                for user in result_rows:
                    enriched_user = {
                        "Login": user["name"],
                        "Type": user["type_desc"],
                        "Created Date": user["create_date"],
                        "Modified Date": user["modify_date"],
                    }
                    enriched_users.append(enriched_user)
            else:
                enriched_users = []

                for user in result_rows:
                    can_impersonate = int(user.get("can_impersonate", 0)) == 1

                    enriched_user = {
                        "Impersonation": "Yes" if can_impersonate else "No",
                        "Login": user["name"],
                        "Type": user["type_desc"],
                        "Created Date": user["create_date"],
                        "Modified Date": user["modify_date"],
                    }
                    enriched_users.append(enriched_user)

            # Display results
            print(OutputFormatter.convert_list_of_dicts(enriched_users))

            logger.info("Use !impersonate <login> to impersonate a user")

            return enriched_users

        except Exception as e:
            logger.error(f"Failed to check impersonation permissions: {e}")
            return None

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            Empty list (no arguments required)
        """
        return []
