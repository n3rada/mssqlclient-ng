# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register("monitor", "Monitor currently running SQL commands")
class Monitor(BaseAction):
    """
    Displays currently running SQL commands on the SQL Server instance.

    Shows session details, command status, wait types, and blocking information.
    """

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        No additional arguments needed for monitoring.

        Args:
            additional_arguments: Ignored.
        """
        pass

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        """
        Executes the monitoring query to show running commands.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            List of currently running SQL commands or None if empty.
        """
        logger.info("Currently running SQL commands")

        current_commands_query = """
            SELECT
                r.session_id AS SessionID,
                r.request_id AS RequestID,
                r.start_time AS StartTime,
                r.status AS Status,
                r.command AS Command,
                DB_NAME(r.database_id) AS DatabaseName,
                r.wait_type AS WaitType,
                r.wait_time AS WaitTime,
                r.blocking_session_id AS BlockingSessionID,
                t.text AS SQLText,
                c.client_net_address AS ClientAddress,
                c.connect_time AS ConnectionStart
            FROM sys.dm_exec_requests r
            CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
            LEFT JOIN sys.dm_exec_connections c
                ON r.session_id = c.session_id
            WHERE r.session_id != @@SPID
            ORDER BY r.start_time DESC;
        """

        results = database_context.query_service.execute_table(current_commands_query)

        if results:
            print(OutputFormatter.convert_list_of_dicts(results))
        else:
            logger.info("No other active SQL commands running")

        return results

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            Empty list as no arguments are required.
        """
        return []
