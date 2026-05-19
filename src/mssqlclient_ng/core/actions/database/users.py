# mssqlclient_ng/core/actions/database/users.py

# Built-in imports

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter

@ActionFactory.register("users", "list server principals and database users")
class Users(BaseAction):
    """
    Enumerates server-level principals (logins) and database users.

    Displays:
    - Server logins with their instance-wide server roles (sysadmin, securityadmin, etc.)
    - Database users in the current database context

    For database-level role memberships, use the 'roles' action instead.
    """

    def execute(self, database_context: DatabaseContext) -> dict | None:
        """
        Executes the user enumeration.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            None
        """
        # Only show server logins on on-premises SQL Server (not Azure SQL Database)
        if not database_context.query_service.is_azure_sql:
            logger.info(
                "Enumerating server-level principals (logins) and their instance-wide server roles"
            )

            if database_context.server.legacy:
                # SQL Server 2016 and earlier: Use STUFF + FOR XML PATH
                server_principals_query = """
                    SELECT
                        sp.name AS Name,
                        sp.type_desc AS type,
                        sp.is_disabled,
                        sp.create_date,
                        sp.modify_date,
                        STUFF((
                            SELECT ', ' + sr.name
                            FROM master.sys.server_role_members srm2
                            INNER JOIN master.sys.server_principals sr ON srm2.role_principal_id = sr.principal_id AND sr.type = 'R'
                            WHERE srm2.member_principal_id = sp.principal_id
                            FOR XML PATH(''), TYPE
                        ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS groups
                    FROM master.sys.server_principals sp
                    WHERE sp.type IN ('G','U','E','S','X') AND sp.name NOT LIKE '##%'
                    ORDER BY sp.is_disabled ASC, sp.modify_date DESC;
                """
            else:
                # SQL Server 2017+: Use STRING_AGG
                server_principals_query = """
                    SELECT
                        sp.name AS Name,
                        sp.type_desc AS type,
                        sp.is_disabled,
                        sp.create_date,
                        sp.modify_date,
                        STRING_AGG(sr.name, ', ') AS groups
                    FROM master.sys.server_principals sp
                    LEFT JOIN master.sys.server_role_members srm ON sp.principal_id = srm.member_principal_id
                    LEFT JOIN master.sys.server_principals sr ON srm.role_principal_id = sr.principal_id AND sr.type = 'R'
                    WHERE sp.type IN ('G','U','E','S','X') AND sp.name NOT LIKE '##%'
                    GROUP BY sp.name, sp.type_desc, sp.is_disabled, sp.create_date, sp.modify_date
                    ORDER BY sp.is_disabled ASC, sp.modify_date DESC;
                """

            raw_table = database_context.query_service.execute_table(
                server_principals_query
            )

            print(OutputFormatter.convert_list_of_dicts(raw_table))

        logger.info("Database users in current database context")

        database_users_query = """
            SELECT name AS username, create_date, modify_date, type_desc AS type,
                   authentication_type_desc AS authentication_type
            FROM sys.database_principals
            WHERE type NOT IN ('R', 'A', 'X') AND sid IS NOT null AND name NOT LIKE '##%'
            ORDER BY modify_date DESC;
        """

        print(
            OutputFormatter.convert_list_of_dicts(
                database_context.query_service.execute_table(database_users_query)
            )
        )

        return raw_table
