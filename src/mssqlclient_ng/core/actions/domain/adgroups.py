# Built-in imports
from typing import Optional, List

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "adgroups",
    "Retrieve Active Directory group memberships that have SQL Server principals",
)
class AdGroups(BaseAction):
    """
    Retrieves Active Directory group memberships that have SQL Server principals.
    Uses IS_MEMBER to check membership, works on both direct connections and linked servers.
    For all Windows token groups (including non-AD), use the AuthToken action instead.
    """

    def __init__(self):
        super().__init__()

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate arguments (none required for adgroups action).

        Args:
            additional_arguments: Ignored, no arguments needed
        """
        # No additional arguments needed
        pass

    def execute(self, database_context: DatabaseContext) -> Optional[List[dict]]:
        """
        Execute the AD groups retrieval.

        Args:
            database_context: The database context containing query_service

        Returns:
            List of AD group dictionaries or None
        """
        logger.info("Retrieving Active Directory groups with SQL Server access")

        try:
            # Check if it's a domain user
            if not database_context.user_service.is_domain_user:
                logger.warning("Current user is not a Windows domain user.")
                return None

            group_names = []

            # Query all AD groups from server principals and check membership with IS_MEMBER
            logger.info("Checking Active Directory group memberships via IS_MEMBER")
            logger.info(
                "  Only showing AD domain groups that exist as SQL Server principals"
            )
            logger.info(
                "  For all Windows token groups (BUILTIN, NT AUTHORITY, etc.), use 'authtoken' action"
            )

            groups_query = """
                SELECT name
                FROM master.sys.server_principals
                WHERE type = 'G'
                AND name LIKE '%\\%'
                AND name NOT LIKE 'BUILTIN\\%'
                AND name NOT LIKE 'NT AUTHORITY\\%'
                AND name NOT LIKE 'NT SERVICE\\%'
                AND name NOT LIKE '##%'
                ORDER BY name;
            """

            server_groups = database_context.query_service.execute_table(groups_query)

            for row in server_groups:
                group_name = row["name"]

                try:
                    escaped_group = group_name.replace("'", "''")
                    member_check_query = f"SELECT IS_MEMBER('{escaped_group}');"
                    result = database_context.query_service.execute_scalar(
                        member_check_query
                    )

                    if result is not None and int(result) == 1:
                        group_names.append(group_name)
                except:
                    # IS_MEMBER might fail for some groups, skip silently
                    pass

            if not group_names:
                logger.warning("User is not a member of any domain groups.")
                return None

            print()
            logger.success(f"Found {len(group_names)} group membership(s)")

            # Query additional details for each group
            groups = []

            for group_name in group_names:
                try:
                    escaped_group = group_name.replace("'", "''")
                    details_query = f"""
                        SELECT type_desc, is_disabled
                        FROM master.sys.server_principals
                        WHERE name = '{escaped_group}';
                    """

                    details = database_context.query_service.execute_table(
                        details_query
                    )

                    if details:
                        groups.append(
                            {
                                "Group Name": group_name,
                                "Type": details[0].get("type_desc", "Windows Group"),
                                "Is Disabled": str(
                                    details[0].get("is_disabled", "Unknown")
                                ),
                            }
                        )
                    else:
                        groups.append(
                            {
                                "Group Name": group_name,
                                "Type": "Windows Group",
                                "Is Disabled": "Unknown",
                            }
                        )
                except:
                    groups.append(
                        {
                            "Group Name": group_name,
                            "Type": "Windows Group",
                            "Is Disabled": "Unknown",
                        }
                    )

            # Display results
            print(OutputFormatter.convert_list_of_dicts(groups))

            return groups

        except Exception as e:
            logger.error(f"Failed to retrieve AD group memberships: {e}")
            return None

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            Empty list as no arguments are required
        """
        return []
