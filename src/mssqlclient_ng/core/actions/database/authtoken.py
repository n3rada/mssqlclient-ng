"""
Retrieves all group memberships from the Windows authentication token.
"""

from typing import Optional, List, Dict, Any

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter
from ...utils.common import convert_table_to_dicts


@ActionFactory.register("authtoken", "Retrieve Windows authentication token groups")
class AuthToken(BaseAction):
    """
    Retrieves all group memberships from the Windows authentication token.
    This includes AD groups, BUILTIN groups, NT AUTHORITY groups, and other Windows security principals.
    Only available for Windows authenticated connections (not available through linked servers).
    """

    ACTION_NAME = "authtoken"
    DESCRIPTION = "Retrieve Windows authentication token groups"

    def get_arguments(self) -> List[str]:
        """
        Return the list of arguments this action expects.

        Returns:
            Empty list - no arguments needed
        """
        return []

    def validate_arguments(self, additional_arguments: str = "") -> None:
        """
        Validate the arguments for the authtoken action.

        Args:
            additional_arguments: Not used for this action

        Raises:
            ValueError: Never raised - no arguments required
        """
        # No additional arguments needed
        pass

    def execute(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, str]]]:
        """
        Execute the authtoken action to retrieve Windows authentication token groups.

        Args:
            database_context: The database context to use for execution

        Returns:
            List of dictionaries containing group information, or None on error
        """
        logger.info("Retrieving Windows authentication token groups")

        try:
            # Check if it's a domain user
            if not database_context.user_service.is_domain_user:
                logger.warning("Current user is not a Windows domain user.")
                return None

            # Query sys.login_token for all groups
            token_query = """
                SELECT DISTINCT
                    lt.name,
                    lt.type,
                    lt.usage,
                    lt.principal_id,
                    sp.name AS sql_principal_name
                FROM sys.login_token lt
                LEFT JOIN master.sys.server_principals sp ON lt.principal_id = sp.principal_id
                WHERE lt.type = 'WINDOWS GROUP'
                ORDER BY lt.name;
            """

            token_rows = database_context.query_service.execute_table(token_query)

            if not token_rows or len(token_rows) == 0:
                logger.warning("No groups found in authentication token.")
                return None

            groups: List[Dict[str, str]] = []

            for row in token_rows:
                group_name = str(row.get("name", ""))
                type_desc = str(row.get("type", ""))
                usage = str(row.get("usage", ""))
                principal_id = row.get("principal_id", 0)

                # Determine group category
                category = self._determine_group_category(group_name)

                # Get SQL Server principal name from the joined query
                sql_principal = row.get("sql_principal_name")
                sql_principal_str = "-" if sql_principal is None else str(sql_principal)

                groups.append(
                    {
                        "Group Name": group_name,
                        "Category": category,
                        "Type": type_desc,
                        "Usage": usage,
                        "SQL Principal": sql_principal_str,
                    }
                )

            # Display results
            headers = ["Group Name", "Category", "Type", "Usage", "SQL Principal"]
            table_data = [
                [
                    group["Group Name"],
                    group["Category"],
                    group["Type"],
                    group["Usage"],
                    group["SQL Principal"],
                ]
                for group in groups
            ]

            print(
                OutputFormatter.convert_list_of_dicts(
                    convert_table_to_dicts(headers, table_data)
                )
            )

            logger.success(
                f"Retrieved {len(groups)} group membership(s) from authentication token"
            )

            return groups

        except Exception as e:
            logger.error(f"Failed to retrieve authentication token: {e}")
            logger.debug(f"Stack trace:", exc_info=True)
            return None

    def _determine_group_category(self, group_name: str) -> str:
        """
        Determines the category of a Windows group based on its name prefix.

        Args:
            group_name: The Windows group name

        Returns:
            Category string: "Built-in", "Well-known SID", "Service", "Active Directory", or "Other"
        """
        if group_name.upper().startswith("BUILTIN\\"):
            return "Built-in"

        if group_name.upper().startswith("NT AUTHORITY\\"):
            return "Well-known SID"

        if group_name.upper().startswith("NT SERVICE\\"):
            return "Service"

        if "\\" in group_name:
            return "Active Directory"

        return "Other"
