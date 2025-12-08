# mssqlclient_ng/core/actions/database/loginmap.py

# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "loginmap", "Map server logins to database users across all accessible databases"
)
class LoginMap(BaseAction):
    """
    Maps server logins to database users across all accessible databases.

    Shows which server-level principals (logins) can access which databases
    and what database user they are mapped to. This is critical for understanding:
    - Cross-database access patterns
    - Orphaned users (database users without corresponding logins)
    - Login-to-user name mismatches
    - Actual database access vs. HAS_DBACCESS permissions

    Usage:
    - No argument: Show all login-to-user mappings
    - With login name: Show mappings only for specified server login

    Note: Only shows mappings for databases where you have access.
    """

    def __init__(self):
        super().__init__()
        self._login_filter: Optional[str] = None

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the login filter argument.

        Args:
            additional_arguments: Optional server login name to filter mappings
        """
        if additional_arguments and additional_arguments.strip():
            named_args, positional_args = self._parse_action_arguments(
                additional_arguments.strip()
            )
            # Get login filter from positional argument
            if positional_args:
                self._login_filter = positional_args[0]

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        """
        Executes the login-to-user mapping.

        Args:
            database_context: The DatabaseContext instance

        Returns:
            List of login-to-user mappings
        """
        is_azure_sql = database_context.query_service.is_azure_sql

        if is_azure_sql:
            logger.warning(
                "Login-to-user mapping not available on Azure SQL Database (PaaS)"
            )
            logger.warning("Azure SQL Database uses contained database users")
            return None

        logger.info(
            "Mapping server logins to database users across all accessible databases"
        )

        query = """
            DECLARE @mapping TABLE (
                [Database] NVARCHAR(128),
                [Server Login] NVARCHAR(128),
                [Login Type] NVARCHAR(60),
                [Database User] NVARCHAR(128),
                [User Type] NVARCHAR(60),
                [Effective Access Via] NVARCHAR(128)
            );

            DECLARE @dbname NVARCHAR(128);
            DECLARE @sql NVARCHAR(MAX);

            DECLARE db_cursor CURSOR FOR
            SELECT name FROM master.sys.databases
            WHERE HAS_DBACCESS(name) = 1
            AND state_desc = 'ONLINE'
            AND name NOT IN ('tempdb', 'model');

            OPEN db_cursor;
            FETCH NEXT FROM db_cursor INTO @dbname;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @sql = N'
                SELECT
                    ''' + @dbname + ''' AS [Database],
                    sp.name AS [Server Login],
                    sp.type_desc AS [Login Type],
                    dp.name AS [Database User],
                    dp.type_desc AS [User Type],
                    CASE
                        WHEN sp.name != dp.name
                            AND EXISTS (
                                SELECT 1 FROM master.sys.login_token lt
                                WHERE lt.sid = dp.sid AND lt.type = ''WINDOWS GROUP''
                            )
                        THEN (
                            SELECT TOP 1 lt.name
                            FROM master.sys.login_token lt
                            WHERE lt.sid = dp.sid AND lt.type = ''WINDOWS GROUP''
                        )
                        ELSE ''Direct''
                    END AS [Effective Access Via]
                FROM [' + @dbname + '].sys.database_principals dp
                INNER JOIN master.sys.server_principals sp ON dp.sid = sp.sid
                WHERE dp.type IN (''S'', ''U'', ''G'', ''E'', ''X'')
                AND dp.name NOT LIKE ''##%''
                AND dp.name NOT IN (''INFORMATION_SCHEMA'', ''sys'', ''guest'')';

                BEGIN TRY
                    INSERT INTO @mapping
                    EXEC sp_executesql @sql;
                END TRY
                BEGIN CATCH
                    -- Skip databases where we don't have permission
                END CATCH

                FETCH NEXT FROM db_cursor INTO @dbname;
            END;

            CLOSE db_cursor;
            DEALLOCATE db_cursor;

            SELECT * FROM @mapping
            ORDER BY [Database], [Server Login];
        """

        try:
            results = database_context.query_service.execute_table(query)

            if not results:
                logger.warning("No login-to-user mappings found")
                return None

            # Apply Python filtering if login filter specified
            if self._login_filter:
                filter_lower = self._login_filter.lower()
                filtered_results = [
                    row
                    for row in results
                    if row["Server Login"].lower() == filter_lower
                    or row["Database User"].lower() == filter_lower
                ]

                if not filtered_results:
                    logger.warning(
                        f"No mappings found for login '{self._login_filter}'"
                    )
                    return None

                results = filtered_results
                logger.info(f"Filtered for login: '{self._login_filter}'")

            # Sort by database and server login
            results_sorted = sorted(
                results,
                key=lambda x: (x["Database"], x["Server Login"]),
            )

            print(OutputFormatter.convert_list_of_dicts(results_sorted))

            logger.success("Login-to-user mapping completed")

            return results_sorted

        except Exception as ex:
            logger.error(f"Error mapping logins to users: {ex}")
            return None

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List containing the optional login filter argument
        """
        return ["[login_name]"]
