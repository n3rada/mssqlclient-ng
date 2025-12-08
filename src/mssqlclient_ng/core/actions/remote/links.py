# mssqlclient_ng/core/actions/remote/links.py

# Built-in imports
from typing import Optional, List, Dict, Any

# Third-party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register("links", "Enumerate linked SQL servers and configurations")
class Links(BaseAction):
    """
    Enumerate linked SQL servers and their configurations.

    Lists all linked servers configured on the SQL Server instance along with
    their authentication mappings, access settings (RPC Out, OPENQUERY), and
    collation compatibility.
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
        Execute the linked servers enumeration.

        Args:
            database_context: The database context

        Returns:
            List of linked servers with their configurations
        """
        logger.info("Retrieving linked SQL servers")

        try:
            result_rows = self._get_linked_servers(database_context)

            if not result_rows:
                logger.warning("No linked servers found")
                return None

            logger.success(f"Found {len(result_rows)} linked server(s)")
            print(OutputFormatter.convert_list_of_dicts(result_rows))

            return result_rows

        except Exception as e:
            logger.error(f"Failed to retrieve linked servers: {e}")
            return None

    @staticmethod
    def _get_linked_servers(
        database_context: DatabaseContext,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve linked servers and login mappings.

        Args:
            database_context: The database context

        Returns:
            List of linked server dictionaries
        """
        query = """
            SELECT
                srv.modify_date AS [Last Modified],
                srv.name AS [Link],
                srv.product AS [Product],
                srv.provider AS [Provider],
                srv.data_source AS [Data Source],
                COALESCE(prin.name, 'N/A') AS [Local Login],
                ll.remote_name AS [Remote Login],
                srv.is_rpc_out_enabled AS [RPC Out],
                srv.is_data_access_enabled AS [OPENQUERY],
                srv.is_collation_compatible AS [Collation]
            FROM sys.servers srv
            LEFT JOIN sys.linked_logins ll ON srv.server_id = ll.server_id
            LEFT JOIN sys.server_principals prin ON ll.local_principal_id = prin.principal_id
            WHERE srv.is_linked = 1
            ORDER BY srv.modify_date DESC;
        """

        return database_context.query_service.execute_table(query)

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            Empty list (no arguments required)
        """
        return []
