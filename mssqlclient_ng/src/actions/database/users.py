# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils.formatters import OutputFormatter


@ActionFactory.register("users", "List server principals and database users")
class Users(BaseAction):
    """
    Displays server-level principals (logins) and database-level users.

    Shows login details, role memberships, and database user information.
    """

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        No additional arguments needed for user enumeration.

        Args:
            additional_arguments: Ignored.
        """
        pass

    def execute(self, database_context: DatabaseContext) -> Optional[dict]:
        """
        Executes the user enumeration.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            Dictionary with server principals and database users.
        """
        logger.info("Server principals with role memberships")

        server_principals_query = """
            SELECT r.name AS Name, r.type_desc AS Type, r.is_disabled, r.create_date, r.modify_date,
                   sl.sysadmin, sl.securityadmin, sl.serveradmin, sl.setupadmin,
                   sl.processadmin, sl.diskadmin, sl.dbcreator, sl.bulkadmin
            FROM master.sys.server_principals r
            LEFT JOIN master.sys.syslogins sl ON sl.sid = r.sid
            WHERE r.type IN ('G','U','E','S','X') AND r.name NOT LIKE '##%'
            ORDER BY r.modify_date DESC;
        """

        server_principals = database_context.query_service.execute_table(
            server_principals_query
        )

        if server_principals:
            print(OutputFormatter.convert_list_of_dicts(server_principals))
        else:
            logger.warning("No server principals found")

        logger.info("Database users")

        database_users_query = """
            SELECT name AS username, create_date, modify_date, type_desc AS type,
                   authentication_type_desc AS authentication_type
            FROM sys.database_principals
            WHERE type NOT IN ('R', 'A', 'X') AND sid IS NOT null AND name NOT LIKE '##%'
            ORDER BY modify_date DESC;
        """

        database_users = database_context.query_service.execute_table(
            database_users_query
        )

        if database_users:
            print(OutputFormatter.convert_list_of_dicts(database_users))
        else:
            logger.warning("No database users found")

        return {
            "server_principals": server_principals,
            "database_users": database_users,
        }

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            Empty list as no arguments are required.
        """
        return []
