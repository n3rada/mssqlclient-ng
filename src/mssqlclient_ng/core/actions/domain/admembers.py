"""
Retrieves members of a specific Active Directory group using multiple methods.
"""

from typing import Optional, List, Dict


from loguru import logger

from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatter import OutputFormatter


@ActionFactory.register(
    "admembers",
    "Retrieves members of a specific Active Directory group",
)
class AdMembers(BaseAction):
    """
    Retrieves members of a specific Active Directory group using multiple methods.
    First tries xp_logininfo (most common method), then optionally tries OPENQUERY with ADSI.
    """

    def __init__(self):
        """Initialize the AdMembers action."""
        super().__init__()
        self._group_name: Optional[str] = None
        self._use_openquery: bool = False

    def validate_arguments(self, args: List[str]) -> bool:
        """
        Validate that a group name is provided and check for openquery flag.

        Args:
            args: List of command line arguments

        Returns:
            bool: True if validation succeeds

        Raises:
            ValueError: If group name is missing or invalid format
        """
        if not args or len(args) == 0:
            raise ValueError(
                "Group name is required. Example: DOMAIN\\IT or DOMAIN\\Domain Admins"
            )

        self._group_name = args[0].strip()

        # Check for openquery flag
        if len(args) > 1 and args[1].strip().lower() == "openquery":
            self._use_openquery = True

        # Ensure the group name contains a backslash (domain separator)
        if "\\" not in self._group_name:
            raise ValueError("Group name must be in format: DOMAIN\\GroupName")

        return True

    def execute(self, db_context: DatabaseContext) -> Optional[List[Dict]]:
        """
        Execute the AD group member enumeration action.

        Args:
            db_context: Database context with connection and services

        Returns:
            Optional[List[Dict]]: List of group members or None if enumeration fails
        """
        logger.info(f"Retrieving members of AD group: {self._group_name}")

        # Try xp_logininfo first (most common method)
        result = self._try_xp_logininfo(db_context)

        if result is not None:
            return result

        # If xp_logininfo fails and openquery flag is set, try OPENQUERY with ADSI
        if self._use_openquery:
            logger.info("Attempting OPENQUERY method with ADSI...")
            result = self._try_openquery_adsi(db_context)

            if result is not None:
                return result

        logger.error("All enumeration methods failed.")
        return None

    def _try_xp_logininfo(self, db_context: DatabaseContext) -> Optional[List[Dict]]:
        """
        Try to enumerate group members using xp_logininfo (default method).

        Args:
            db_context: Database context with connection and services

        Returns:
            Optional[List[Dict]]: List of group members or None if method fails
        """
        try:
            logger.info("Trying xp_logininfo method...")

            # Check if xp_logininfo is available
            xproc_check = db_context.query_service.execute_table(
                "SELECT * FROM master.sys.all_objects WHERE name = 'xp_logininfo' AND type = 'X';"
            )

            if not xproc_check:
                logger.warning(
                    "xp_logininfo extended stored procedure is not available."
                )
                return None

            # Escape single quotes in group name to prevent SQL injection
            escaped_group_name = self._group_name.replace("'", "''")

            # Query group members using xp_logininfo
            query = f"EXEC xp_logininfo @acctname = '{escaped_group_name}', @option = 'members';"
            members_table = db_context.query_service.execute_table(query)

            if not members_table:
                logger.warning(
                    f"No members found for group '{self._group_name}'. Verify the group name and permissions."
                )
                return None

            logger.success(f"Found {len(members_table)} member(s) using xp_logininfo")
            print(OutputFormatter.convert_list_of_dicts(members_table))

            return members_table

        except Exception as ex:
            logger.warning(f"xp_logininfo method failed: {ex}")
            return None

    def _try_openquery_adsi(self, db_context: DatabaseContext) -> Optional[List[Dict]]:
        """
        Try to enumerate group members using OPENQUERY with ADSI.
        Requires 'Ad Hoc Distributed Queries' to be enabled.

        Args:
            db_context: Database context with connection and services

        Returns:
            Optional[List[Dict]]: List of group members or None if method fails
        """
        try:
            # Extract domain and group name
            parts = self._group_name.split("\\")
            if len(parts) != 2:
                logger.warning("Invalid group name format for OPENQUERY method.")
                return None

            domain = parts[0]
            group_name = parts[1]

            # Convert domain to DC format (e.g., example.com -> DC=example,DC=com)
            dc_format = ",DC=".join(domain.split("."))

            # Build LDAP query
            # Note: This assumes the group is in CN=Users, adjust if needed
            query = f"""
                SELECT *
                FROM OPENQUERY(
                    ADSI,
                    'SELECT cn, sAMAccountName, distinguishedName
                     FROM ''LDAP://{domain}''
                     WHERE objectClass = ''user''
                     AND memberOf = ''CN={group_name},CN=Users,DC={dc_format}'''
                );
            """

            members_table = db_context.query_service.execute_table(query)

            if not members_table:
                logger.warning("No members found using OPENQUERY method.")
                return None

            logger.success(f"Found {len(members_table)} member(s) using OPENQUERY/ADSI")
            print(OutputFormatter.convert_list_of_dicts(members_table))

            return members_table

        except Exception as ex:
            logger.warning(f"OPENQUERY/ADSI method failed: {ex}")
            return None
