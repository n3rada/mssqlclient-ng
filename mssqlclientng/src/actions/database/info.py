"""
Info action for retrieving SQL Server instance information using DMVs and SERVERPROPERTY.
"""

from typing import Optional, Dict, Any
from loguru import logger

from mssqlclientng.src.actions.base import BaseAction
from mssqlclientng.src.actions.factory import ActionFactory
from mssqlclientng.src.services.database import DatabaseContext
from mssqlclientng.src.utils import formatter


# Information queries using DMVs and SERVERPROPERTY (no registry access)
INFO_QUERIES = {
    "Server Name": "SELECT @@SERVERNAME;",
    "Default Domain": "SELECT DEFAULT_DOMAIN();",
    "Host Name": "SELECT SERVERPROPERTY('MachineName');",
    "Operating System Version": "SELECT TOP(1) windows_release + ISNULL(' ' + windows_service_pack_level, '') FROM sys.dm_os_windows_info;",
    "SQL Service Process ID": "SELECT SERVERPROPERTY('ProcessId');",
    "SQL Service Account": "SELECT SERVERPROPERTY('ServiceAccountName');",
    "Instance Name": "SELECT ISNULL(SERVERPROPERTY('InstanceName'), 'DEFAULT');",
    "Authentication Mode": "SELECT CASE SERVERPROPERTY('IsIntegratedSecurityOnly') WHEN 1 THEN 'Windows Authentication' ELSE 'Mixed Authentication' END;",
    "Clustered Server": "SELECT CASE SERVERPROPERTY('IsClustered') WHEN 0 THEN 'No' ELSE 'Yes' END;",
    "SQL Version": "SELECT SERVERPROPERTY('ProductVersion');",
    "SQL Major Version": "SELECT SUBSTRING(@@VERSION, CHARINDEX('2', @@VERSION), 4);",
    "SQL Edition": "SELECT SERVERPROPERTY('Edition');",
    "SQL Service Pack": "SELECT SERVERPROPERTY('ProductLevel');",
    "OS Architecture": "SELECT SUBSTRING(@@VERSION, CHARINDEX('x', @@VERSION), 3);",
    "OS Version Number": "SELECT RIGHT(SUBSTRING(@@VERSION, CHARINDEX('Windows Server', @@VERSION), 19), 4);",
    "Logged-in User": "SELECT SYSTEM_USER;",
    "Active SQL Sessions": "SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE status = 'running';",
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
