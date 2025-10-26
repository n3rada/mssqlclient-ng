# Standard library imports
from typing import Optional, List, Dict, Any

# Third-party imports
from loguru import logger

# Local library imports
from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.utils.formatter import rows_to_markdown_table


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

        Raises:
            ValueError: If unexpected arguments are provided
        """
        if additional_arguments and additional_arguments.strip():
            logger.warning(
                "Sessions action does not accept arguments, ignoring: %s",
                additional_arguments,
            )

    def execute(self, database_context=None) -> Optional[List[Dict[str, Any]]]:
        """
        Execute the query to retrieve active sessions.

        Args:
            database_context: The database context containing query_service

        Returns:
            List of session rows from the query
        """
        query_service = database_context.query_service

        logger.info("Active SQL Server sessions")

        sessions_query = """
            SELECT
                session_id,
                login_time,
                host_name,
                program_name,
                client_interface_name,
                login_name
            FROM sys.dm_exec_sessions
            ORDER BY login_time DESC;
        """

        result_rows = query_service.execute_table(sessions_query)

        if not result_rows:
            logger.warning("No active sessions found")
            return result_rows

        logger.success(f"Active sessions: {len(result_rows)}")

        # Display as markdown table
        result = rows_to_markdown_table(result_rows)

        print()
        print(result)

        return result_rows

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions (empty, no arguments needed)
        """
        return []
