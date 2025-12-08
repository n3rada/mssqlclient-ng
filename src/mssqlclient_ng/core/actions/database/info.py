# mssqlclient_ng/core/actions/database/info.py

# Built-in imports
import re
from typing import Optional, Dict, List

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


# Query sets organized by environment type
INFO_QUERIES = {
    "all": {
        # Server Identification
        "Server Name": "SELECT @@SERVERNAME;",
        "Instance Name": "SELECT ISNULL(CAST(SERVERPROPERTY('InstanceName') AS NVARCHAR(256)), 'DEFAULT');",
        "Computer Name": "SELECT CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS NVARCHAR(256));",
        "Default Domain": "SELECT DEFAULT_DOMAIN();",
        "Current Database": "SELECT DB_NAME();",
        # SQL Server Information
        "SQL Version": "SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(256));",
        "SQL Major Version": "SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS INT);",
        "SQL Edition": "SELECT CAST(SERVERPROPERTY('Edition') AS NVARCHAR(256));",
        "SQL Service Pack": "SELECT CAST(SERVERPROPERTY('ProductLevel') AS NVARCHAR(256));",
        # Configuration
        "Authentication Mode": "SELECT CASE CAST(SERVERPROPERTY('IsIntegratedSecurityOnly') AS INT) WHEN 1 THEN 'Windows Authentication only' ELSE 'Mixed mode (Windows + SQL)' END;",
        "Clustered Server": "SELECT CASE CAST(SERVERPROPERTY('IsClustered') AS INT) WHEN 0 THEN 'No' ELSE 'Yes' END;",
        # Full Version
        "Full Version String": "SELECT @@VERSION;",
    },
    "on-premises": {
        "Host Name": "SELECT CAST(SERVERPROPERTY('MachineName') AS NVARCHAR(256));",
        "SQL Service Process ID": "SELECT CAST(SERVERPROPERTY('ProcessId') AS INT);",
        "Operating System Version": "SELECT TOP(1) windows_release + ISNULL(' ' + windows_service_pack_level, '') FROM master.sys.dm_os_windows_info;",
        "OS Architecture": "SELECT CASE WHEN CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) LIKE '%64%' THEN '64-bit' ELSE '32-bit' END;",
    },
    "azure": {
        "Azure Service Tier": "SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'ServiceObjective') AS NVARCHAR(256));",
        "Azure Database Edition": "SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'Edition') AS NVARCHAR(256));",
        "Azure Max Database Size": "SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes') AS BIGINT);",
        "Azure Engine Edition": "SELECT CAST(SERVERPROPERTY('EngineEdition') AS INT);",
    },
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
        is_azure = database_context.query_service.is_azure_sql

        # Determine which query sets to use
        query_sets: List[Dict[str, str]] = [INFO_QUERIES["all"]]

        if is_azure:
            query_sets.append(INFO_QUERIES["azure"])
            logger.debug("Detected Azure SQL environment")
        else:
            query_sets.append(INFO_QUERIES["on-premises"])
            logger.debug("Detected on-premises SQL Server environment")

        # Execute all queries from the selected sets
        for query_set in query_sets:
            for key, query in query_set.items():
                try:
                    query_result = database_context.query_service.execute_table(query)

                    # Extract the first row and first column value if present
                    if query_result and len(query_result) > 0:
                        value = query_result[0].get(list(query_result[0].keys())[0])
                        result_value = str(value) if value is not None else "NULL"
                    else:
                        result_value = "NULL"

                    # Special handling for Azure Max Database Size
                    if key == "Azure Max Database Size":
                        try:
                            bytes_value = int(result_value)
                            if bytes_value > 0:
                                gb_value = bytes_value / (1024.0 * 1024.0 * 1024.0)
                                result_value = f"{gb_value:.2f} GB"
                            else:
                                result_value = "Unlimited or default"
                        except (ValueError, TypeError):
                            result_value = "Unlimited or default"

                    # Special handling for Azure Engine Edition
                    if key == "Azure Engine Edition":
                        engine_editions = {
                            "1": "Personal or Desktop Engine",
                            "2": "Standard",
                            "3": "Enterprise",
                            "4": "Express",
                            "5": "Azure SQL Database",
                            "6": "Azure Synapse Analytics",
                            "8": "Azure SQL Managed Instance",
                            "9": "Azure SQL Edge",
                            "11": "Azure Synapse serverless SQL pool",
                        }
                        description = engine_editions.get(result_value, "Unknown")
                        result_value = f"{result_value} ({description})"

                    # Split Full Version String into multiple rows with meaningful labels
                    if key == "Full Version String":
                        lines = result_value.split("\n")
                        for i, line in enumerate(lines):
                            line = line.strip()
                            if not line:
                                continue

                            # Determine the purpose of each line based on its content
                            if line.upper().startswith("MICROSOFT SQL"):
                                line_key = "Product Version"
                            elif line.upper().startswith("COPYRIGHT"):
                                line_key = "Copyright"
                            elif "Edition" in line and "Licensing" in line:
                                line_key = "Edition Details"
                            elif "Windows" in line and (
                                "Server" in line or "Build" in line
                            ):
                                line_key = "OS Details"
                            elif re.match(r"^\w{3}\s+\d{1,2}\s+\d{4}", line):
                                # Matches date patterns like "Oct 7 2025"
                                line_key = "Build Date"
                            else:
                                line_key = f"Version Info (Line {i + 1})"

                            results[line_key] = line
                    else:
                        results[key] = result_value

                except Exception as e:
                    logger.warning(f"Failed to execute '{key}': {e}")
                    results[key] = f"ERROR: {str(e)}"

        logger.success("SQL Server information retrieved")

        # Display results
        print(OutputFormatter.convert_dict(results, "Information", "Value"))

        return results

    def get_arguments(self) -> list:
        """
        Get the list of arguments for this action.

        Returns:
            Empty list (no arguments required)
        """
        return []
