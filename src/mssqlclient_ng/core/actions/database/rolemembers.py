# Built-in imports

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction, Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter

@ActionFactory.register("rolemembers", "List members of a specific server role (e.g., sysadmin).")
class RoleMembers(BaseAction):
    """
    Retrieves all members of a specified server role.

    Common server roles: sysadmin, serveradmin, securityadmin, setupadmin,
    processadmin, diskadmin, dbcreator, bulkadmin, public
    """

    _role_name = Arg(position=0, required=True, description="Server role name (e.g., sysadmin)")

    def execute(self, database_context: DatabaseContext) -> list[dict] | None:
        """
        Executes the role members enumeration.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            None
        """
        logger.info(f"Retrieving members of server role: {self._role_name}")

        # Escape single quotes in role name for SQL injection prevention
        escaped_role_name = self._role_name.replace("'", "''")

        query = f"""
            SELECT
                l.name AS LoginName,
                l.type_desc AS LoginType,
                l.is_disabled AS IsDisabled,
                l.create_date AS CreateDate,
                l.modify_date AS ModifyDate
            FROM master.sys.server_role_members rm
            JOIN master.sys.server_principals r ON rm.role_principal_id = r.principal_id
            JOIN master.sys.server_principals l ON rm.member_principal_id = l.principal_id
            WHERE r.name = '{escaped_role_name}'
            ORDER BY l.create_date DESC;
        """

        result = database_context.query_service.execute_table(query)

        if not result:
            logger.warning(
                f"No members found for role '{self._role_name}'. Verify the role name is correct."
            )
        else:
            logger.success(f"Found {len(result)} member(s) in role '{self._role_name}'")

        print(OutputFormatter.convert_list_of_dicts(result))

        return result
