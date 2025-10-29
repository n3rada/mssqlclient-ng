# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from mssqlclientng.src.actions.base import BaseAction
from mssqlclientng.src.actions.factory import ActionFactory
from mssqlclientng.src.services.database import DatabaseContext
from mssqlclientng.src.utils import formatter


@ActionFactory.register("rolemembers", "List members of a specified server role")
class RoleMembers(BaseAction):
    """
    Retrieves all members of a specified server role.

    Common server roles: sysadmin, serveradmin, securityadmin, setupadmin,
    processadmin, diskadmin, dbcreator, bulkadmin, public
    """

    def __init__(self):
        super().__init__()
        self._role_name: str = ""

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates that a role name has been provided.

        Args:
            additional_arguments: The server role name.

        Raises:
            ValueError: If the role name is empty.
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Role name is required. Example: sysadmin, serveradmin, securityadmin, etc."
            )

        self._role_name = additional_arguments.strip()

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        """
        Executes the role members enumeration.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            List of role members or empty list if none found.
        """
        logger.info(f"Retrieving members of server role: {self._role_name}")

        query = f"""
            SELECT
                l.name AS LoginName,
                l.type_desc AS LoginType,
                l.is_disabled AS IsDisabled,
                l.create_date AS CreateDate,
                l.modify_date AS ModifyDate,
                l.tenant_id AS TenantId
            FROM sys.server_role_members rm
            JOIN sys.server_principals r ON rm.role_principal_id = r.principal_id
            JOIN sys.server_principals l ON rm.member_principal_id = l.principal_id
            WHERE r.name = '{self._role_name}'
            ORDER BY l.create_date DESC;
        """

        try:
            result = database_context.query_service.execute_table(query)

            if not result:
                logger.warning(
                    f"No members found for role '{self._role_name}'. Verify the role name is correct."
                )
                return []

            logger.success(f"Found {len(result)} member(s) in role '{self._role_name}'")
            print(formatter.rows_to_markdown_table(result))

            return result

        except Exception as e:
            logger.error(f"Failed to retrieve role members: {e}")
            raise

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List containing the role name argument.
        """
        return ["role_name"]
