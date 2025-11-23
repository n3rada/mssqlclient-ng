"""
Info action for retrieving SQL Server instance information using DMVs and SERVERPROPERTY.
"""

from typing import Optional, Dict, Any
from loguru import logger

from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils import formatter


INFO_QUERIES = {
    "Server Name": "SELECT @@SERVERNAME;",
    "Default Domain": "SELECT DEFAULT_DOMAIN();",
    "Host Name": "SELECT CAST(SERVERPROPERTY('MachineName') AS NVARCHAR(256));",
    "Operating System Version": "SELECT TOP(1) windows_release + ISNULL(' ' + windows_service_pack_level, '') FROM sys.dm_os_windows_info;",
    "SQL Service Process ID": "SELECT CAST(SERVERPROPERTY('ProcessId') AS INT);",
    "Instance Name": "SELECT ISNULL(CAST(SERVERPROPERTY('InstanceName') AS NVARCHAR(256)), 'DEFAULT');",
    "Authentication Mode": "SELECT CASE CAST(SERVERPROPERTY('IsIntegratedSecurityOnly') AS INT) WHEN 1 THEN 'Windows Authentication only' ELSE 'Mixed mode (Windows + SQL)' END;",
    "Clustered Server": "SELECT CASE CAST(SERVERPROPERTY('IsClustered') AS INT) WHEN 0 THEN 'No' ELSE 'Yes' END;",
    "SQL Version": "SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(256));",
    "SQL Major Version": "SELECT SUBSTRING(@@VERSION, CHARINDEX('2', @@VERSION), 4);",
    "SQL Edition": "SELECT CAST(SERVERPROPERTY('Edition') AS NVARCHAR(256));",
    "SQL Service Pack": "SELECT CAST(SERVERPROPERTY('ProductLevel') AS NVARCHAR(256));",
    "OS Architecture": "SELECT SUBSTRING(@@VERSION, CHARINDEX('x', @@VERSION), 3);",
    "OS Version Number": "SELECT RIGHT(SUBSTRING(@@VERSION, CHARINDEX('Windows Server', @@VERSION), 19), 4);",
}


@ActionFactory.register("info", "Retrieve SQL Server instance information")
class Info(BaseAction):
    """
    Retrieve SQL Server instance information using DMVs and SERVERPROPERTY.

    Gathers server details including version, edition, authentication mode,
    operating system information, and service account. Uses only DMVs and
    built-in functions (no registry access required).
    """

    def __init__(self):
        super().__init__()

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate arguments (none required for this action).

        Args:
            additional_arguments: Not used
        """
        # No arguments needed
        pass

    def execute(self, database_context: DatabaseContext) -> Optional[Dict[str, str]]:
        """
        Execute information gathering queries.

        Args:
            database_context: The database context

        Returns:
            Dictionary of information keys and values
        """
        logger.info("Retrieving SQL Server instance information...")

        results: Dict[str, str] = {}

        for key, query in INFO_QUERIES.items():
            try:
                query_result = database_context.query_service.execute_table(query)

                # Extract the first row and first column value if present
                if query_result and len(query_result) > 0:
                    value = query_result[0].get(list(query_result[0].keys())[0])
                    result_value = str(value) if value is not None else "NULL"
                else:
                    result_value = "NULL"

                results[key] = result_value

            except Exception as e:
                logger.warning(f"Failed to execute '{key}': {e}")
                results[key] = f"ERROR: {str(e)}"

        logger.success("SQL Server information retrieved")

        # Display results
        print(formatter.dict_to_markdown_table(results, "Information", "Value"))

        return results

    def get_arguments(self) -> list:
        """
        Get the list of arguments for this action.

        Returns:
            Empty list (no arguments required)
        """
        return []
