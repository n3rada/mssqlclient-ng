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

    def validate_arguments(self, additional_arguments: str = "") -> None:
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
            # Use the richer sys.databases catalog view (SQL Server 2005+)
            query = """
SELECT
    d.database_id AS [ID],
    d.name AS [Name],
    SUSER_SNAME(d.owner_sid) AS [Owner],
    CAST(HAS_DBACCESS(d.name) AS BIT) AS [Accessible],
    d.is_trustworthy_on AS [Trustworthy],
    d.state_desc AS [State],
    d.user_access_desc AS [Access],
    d.is_read_only AS [ReadOnly],
    d.recovery_model_desc AS [Recovery Model],
    d.create_date AS [Created],
    mf.physical_name AS [MDF Path]
FROM sys.databases d
LEFT JOIN sys.master_files mf
    ON d.database_id = mf.database_id AND mf.file_id = 1
ORDER BY HAS_DBACCESS(d.name) DESC, d.name ASC;"""

            all_databases = database_context.query_service.execute_table(query)

            if not all_databases:
                logger.warning("No databases found")
                return None

            # For each accessible database, check if the current user is db_owner
            for db in all_databases:
                db["db_owner"] = False
                if db.get("Accessible"):
                    db_name = db["Name"]
                    try:
                        owner_check = database_context.query_service.execute_scalar(
                            f"USE [{db_name}]; SELECT CAST(IS_MEMBER('db_owner') AS BIT);"
                        )
                        db["db_owner"] = bool(owner_check)
                    except Exception:
                        pass

            print(OutputFormatter.convert_list_of_dicts(all_databases))
            return all_databases

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
