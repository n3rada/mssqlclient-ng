# mssqlclient_ng/core/actions/database/databases.py

# Built-in imports
from typing import Any

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter

@ActionFactory.register(
    "databases", "List all databases with accessibility, owner, TRUSTWORTHY flag, state, and file paths."
)
class Databases(BaseAction):
    """
    list all SQL Server databases with accessibility and security information.

    Shows database details including creation date, accessibility status,
    trustworthy flag, and owner information.
    """

    def execute(
        self, database_context: DatabaseContext
    ) -> list[dict[str, Any]] | None:
        """
        Execute the databases listing action.

        Args:
            database_context: The database context

        Returns:
            list of database dictionaries with combined information
        """
        try:
            # Use the richer sys.databases catalog view (SQL Server 2005+)
            query = """
SELECT
    d.database_id AS [ID],
    d.create_date AS [Created],
    d.name AS [Name],
    SUSER_SNAME(d.owner_sid) AS [Owner],
    CAST(IS_SRVROLEMEMBER('sysadmin', SUSER_SNAME(d.owner_sid)) AS BIT) AS [OwnerIsSysadmin],
    CAST(HAS_DBACCESS(d.name) AS BIT) AS [Accessible],
    d.is_trustworthy_on AS [Trustworthy],
    d.state_desc AS [State],
    d.user_access_desc AS [Access],
    d.is_read_only AS [ReadOnly],
    d.recovery_model_desc AS [Recovery Model],
    mf.physical_name AS [MDF Path]
FROM sys.databases d
LEFT JOIN sys.master_files mf
    ON d.database_id = mf.database_id AND mf.file_id = 1
ORDER BY HAS_DBACCESS(d.name) DESC, d.name ASC;"""

            all_databases = database_context.query_service.execute_table(query)

            if not all_databases:
                logger.warning("No databases found")
                return None

            # Check db_owner role membership for all accessible databases in one roundtrip
            # IS_MEMBER is context-dependent so we build dynamic SQL with USE per-database
            # EXECUTE() wraps the batch so USE statements don't change the outer context
            db_owner_query = """
DECLARE @sql NVARCHAR(MAX) = N'';
SELECT @sql = @sql +
    N'USE [' + REPLACE(name, ']', ']]') + N']; INSERT INTO #db_owner_check VALUES(''' + REPLACE(name, '''', '''''') + N''', CAST(IS_MEMBER(''db_owner'') AS BIT)); '
FROM sys.databases WHERE HAS_DBACCESS(name) = 1;
CREATE TABLE #db_owner_check (db_name NVARCHAR(256), is_db_owner BIT);
EXECUTE(@sql);
SELECT db_name, is_db_owner FROM #db_owner_check;
DROP TABLE #db_owner_check;"""

            try:
                owner_results = database_context.query_service.execute_table(
                    db_owner_query
                )
                owner_map = {
                    row["db_name"]: bool(row["is_db_owner"])
                    for row in (owner_results or [])
                }
            except Exception:
                owner_map = {}

            for db in all_databases:
                db["db_owner"] = owner_map.get(db["Name"], False)

            print(OutputFormatter.convert_list_of_dicts(all_databases))

            # Flag trustworthy databases owned by sysadmin where current user has db_owner
            # This combination enables privilege escalation via EXECUTE AS OWNER
            exploitable = [
                db
                for db in all_databases
                if db.get("Trustworthy")
                and db.get("OwnerIsSysadmin")
                and db.get("db_owner")
            ]
            for db in exploitable:
                logger.warning(
                    f"Database '{db['Name']}' is TRUSTWORTHY with sysadmin owner '{db['Owner']}' "
                    f"and current user has db_owner role - privilege escalation possible via EXECUTE AS OWNER"
                )

            return all_databases

        except Exception as e:
            logger.error(f"Failed to retrieve database information: {e}")
            return None
