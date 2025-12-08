# mssqlclient_ng/core/actions/database/impersonate.py

from typing import Optional, List, Dict, Any
from loguru import logger

from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "impersonate", "Check which SQL logins can be impersonated by current user"
)
class Impersonation(BaseAction):
    """
    Check SQL Server impersonation permissions.

    Lists all SQL logins and Windows principals, and checks which ones
    can be impersonated by the current user. Sysadmin users can impersonate
    any login.
    """

    def __init__(self):
        super().__init__()

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate arguments (none required for this action).

        Args:
            additional_arguments: Not used
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
            # Query to obtain all SQL logins and Windows principals
            query = """
            SELECT name, type_desc, create_date, modify_date
            FROM sys.server_principals
            WHERE type_desc IN ('SQL_LOGIN', 'WINDOWS_LOGIN') AND name NOT LIKE '##%'
            ORDER BY create_date DESC;
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
                # All logins can be impersonated by sysadmin
                enriched_users = []
                for user in result_rows:
                    enriched_user = {
                        "Impersonation": "Yes",
                        "Login": user["name"],
                        "Type": user["type_desc"],
                        "Created Date": user["create_date"],
                        "Modified Date": user["modify_date"],
                    }
                    enriched_users.append(enriched_user)
            else:
                logger.info("Checking impersonation permissions individually")
                enriched_users = []

                # Get current user to skip checking self
                current_user = database_context.user_service.get_mapped_user()

                for user in result_rows:
                    username = user["name"]

                    # Skip checking impersonation for the current user
                    if username.lower() == current_user.lower():
                        enriched_user = {
                            "Impersonation": "Self",
                            "Login": username,
                            "Type": user["type_desc"],
                            "Created Date": user["create_date"],
                            "Modified Date": user["modify_date"],
                        }
                    else:
                        can_impersonate = database_context.user_service.can_impersonate(
                            username
                        )

                        enriched_user = {
                            "Impersonation": "Yes" if can_impersonate else "No",
                            "Login": username,
                            "Type": user["type_desc"],
                            "Created Date": user["create_date"],
                            "Modified Date": user["modify_date"],
                        }
                    enriched_users.append(enriched_user)

            # Display results
            print(OutputFormatter.convert_list_of_dicts(enriched_users))

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
