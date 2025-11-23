# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils import formatter


@ActionFactory.register(
    "groupmembers", "Retrieve members of a specific Active Directory group"
)
class GroupMembers(BaseAction):
    """
    Retrieves members of a specific Active Directory group using multiple methods.

    Tries xp_logininfo first (default), with optional OPENQUERY/ADSI fallback.
    """

    def __init__(self):
        super().__init__()
        self._group_name: str = ""
        self._use_openquery: bool = False

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the arguments for group member enumeration.

        Args:
            additional_arguments: Group name (e.g., DOMAIN\\Domain Admins) [openquery]

        Raises:
            ValueError: If arguments are invalid.
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Group name is required. Example: DOMAIN\\IT or DOMAIN\\Domain Admins"
            )

        parts = additional_arguments.strip().split()
        self._group_name = parts[0].strip()

        # Check for openquery flag
        if len(parts) > 1 and parts[1].strip().lower() == "openquery":
            self._use_openquery = True

        # Ensure the group name contains a backslash (domain separator)
        if "\\" not in self._group_name:
            raise ValueError("Group name must be in format: DOMAIN\\GroupName")

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        """
        Executes the group member enumeration.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            List of group members or None if enumeration fails.
        """
        logger.info(f"Retrieving members of AD group: {self._group_name}")

        # Try xp_logininfo first (most common method)
        result = self._try_xp_logininfo(database_context)

        if result is not None:
            return result

        # If xp_logininfo fails and openquery flag is set, try OPENQUERY with ADSI
        if self._use_openquery:
            logger.info("Attempting OPENQUERY method with ADSI")
            result = self._try_openquery_adsi(database_context)

            if result is not None:
                return result

        logger.error("All enumeration methods failed")
        return None

    def _try_xp_logininfo(
        self, database_context: DatabaseContext
    ) -> Optional[list[dict]]:
        """
        Tries to enumerate group members using xp_logininfo (default method).

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            List of group members or None if method fails.
        """
        try:
            logger.info("Trying xp_logininfo method")

            # Check if xp_logininfo is available
            xproc_check = database_context.query_service.execute_table(
                "SELECT * FROM sys.all_objects WHERE name = 'xp_logininfo' AND type = 'X';"
            )

            if not xproc_check:
                logger.warning(
                    "xp_logininfo extended stored procedure is not available"
                )
                return None

            # Query group members using xp_logininfo
            query = f"EXEC xp_logininfo @acctname = '{self._group_name}', @option = 'members';"
            members_table = database_context.query_service.execute_table(query)

            if not members_table:
                logger.warning(
                    f"No members found for group '{self._group_name}'. "
                    "Verify the group name and permissions"
                )
                return None

            logger.success(f"Found {len(members_table)} member(s) using xp_logininfo")

            # Display the results
            print(formatter.rows_to_markdown_table(members_table))

            return members_table

        except Exception as ex:
            logger.warning(f"xp_logininfo method failed: {ex}")
            return None

    def _try_openquery_adsi(
        self, database_context: DatabaseContext
    ) -> Optional[list[dict]]:
        """
        Tries to enumerate group members using OPENQUERY with ADSI.
        Requires 'Ad Hoc Distributed Queries' to be enabled.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            List of group members or None if method fails.
        """
        try:
            # Extract domain and group name
            parts = self._group_name.split("\\")
            if len(parts) != 2:
                logger.warning("Invalid group name format for OPENQUERY method")
                return None

            domain = parts[0]
            group_name = parts[1]

            # Build LDAP query
            dc_parts = ",DC=".join(domain.split("."))
            query = f"""
                SELECT *
                FROM OPENQUERY(
                    ADSI,
                    'SELECT cn, sAMAccountName, distinguishedName
                     FROM ''LDAP://{domain}''
                     WHERE objectClass = ''user''
                     AND memberOf = ''CN={group_name},CN=Users,DC={dc_parts}'''
                );
            """

            members_table = database_context.query_service.execute_table(query)

            if not members_table:
                logger.warning("No members found using OPENQUERY method")
                return None

            logger.success(f"Found {len(members_table)} member(s) using OPENQUERY/ADSI")
            print(formatter.rows_to_markdown_table(members_table))

            return members_table

        except Exception as ex:
            logger.warning(f"OPENQUERY/ADSI method failed: {ex}")
            return None

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List of argument descriptions.
        """
        return ["DOMAIN\\GroupName [openquery]"]
