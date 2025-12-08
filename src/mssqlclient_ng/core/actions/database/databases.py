# mssqlclient_ng/core/actions/database/databases.py

# Built-in imports
from typing import Optional, List, Dict, Any

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "databases", "List all databases with access and security information"
)
class Databases(BaseAction):
    """
    List all SQL Server databases with accessibility and security information.

    Shows database details including creation date, accessibility status,
    trustworthy flag, and owner information.
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

    def execute(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute the databases listing action.

        Args:
            database_context: The database context

        Returns:
            List of database dictionaries with combined information
        """
        try:
            # Query for all databases
            all_databases = database_context.query_service.execute_table(
                "SELECT dbid, name, crdate, filename FROM master.dbo.sysdatabases ORDER BY crdate DESC;"
            )

            if not all_databases:
                logger.warning("No databases found")
                return None

            # Query for accessible databases
            accessible_databases = database_context.query_service.execute_table(
                "SELECT name FROM sys.databases WHERE HAS_DBACCESS(name) = 1;"
            )

            # Query for trustworthy databases and owner information
            database_info = database_context.query_service.execute_table(
                """SELECT
                    d.name,
                    d.is_trustworthy_on,
                    SUSER_SNAME(d.owner_sid) AS owner_name
                FROM sys.databases d;"""
            )

            # Create sets for quick lookup
            accessible_set = (
                {row["name"] for row in accessible_databases}
                if accessible_databases
                else set()
            )

            # Create dict for trustworthy and owner info
            info_dict = {}
            if database_info:
                for row in database_info:
                    info_dict[row["name"]] = {
                        "trustworthy": row["is_trustworthy_on"],
                        "owner": row["owner_name"],
                    }

            # Add accessibility, trustworthy, and owner columns to each database
            enriched_databases = []
            for db in all_databases:
                db_name = db["name"]

                # Create enriched database entry with reordered columns
                enriched_db = {
                    "dbid": db["dbid"],
                    "name": db_name,
                    "Accessible": db_name in accessible_set,
                    "Trustworthy": info_dict.get(db_name, {}).get("trustworthy", False),
                    "Owner": info_dict.get(db_name, {}).get("owner", ""),
                    "crdate": db["crdate"],
                    "filename": db["filename"],
                }

                enriched_databases.append(enriched_db)

            # Display the table
            print(OutputFormatter.convert_list_of_dicts(enriched_databases))

            return enriched_databases

        except Exception as e:
            logger.error(f"Failed to retrieve database information: {e}")
            return None

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            Empty list (no arguments required)
        """
        return []
