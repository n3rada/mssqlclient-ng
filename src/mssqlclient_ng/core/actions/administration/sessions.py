# Standard library imports
from typing import Optional, List, Dict, Any

# Third-party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...utils.formatters import OutputFormatter


@ActionFactory.register("sessions", "List active SQL Server sessions")
class Sessions(BaseAction):
    """
    Display active SQL Server sessions with connection information.

    Shows details about all active sessions including session ID, login time,
    host name, program name, client interface, and login name.

    Usage:
        sessions      # List all active sessions
    """

    def __init__(self):
        super().__init__()

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate arguments (none required for sessions action).

        Args:
            additional_arguments: Ignored, no arguments needed
        """
        # No additional arguments needed
        pass

    def execute(self, database_context=None) -> Optional[List[Dict[str, Any]]]:
        """
        Execute the query to retrieve active sessions.

        Args:
            database_context: The database context containing query_service

        Returns:
            None
        """
        logger.info("Retrieving active SQL Server sessions")

        sessions_query = """
            SELECT
                session_id,
                login_time,
                host_name,
                program_name,
                client_interface_name,
                login_name
            FROM master.sys.dm_exec_sessions
            ORDER BY login_time DESC;
        """

        result = database_context.query_service.execute(sessions_query)
        print(OutputFormatter.convert_sql_data_reader(result))

        return None

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions (empty, no arguments needed)
        """
        return []
