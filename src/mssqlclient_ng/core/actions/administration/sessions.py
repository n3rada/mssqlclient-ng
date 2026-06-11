# mssqlclient_ng/core/actions/administration/sessions.py

# Standard library imports
from typing import Any

# Third-party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...utils.formatters import OutputFormatter

@ActionFactory.register("sessions", "Display active SQL Server sessions with login and connection information.", aliases=["who"])
class Sessions(BaseAction):

    def execute(self, database_context=None) -> list[dict[str, Any]] | None:
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

        result = database_context.query_service.execute_table(sessions_query)
        print(OutputFormatter.convert_list_of_dicts(result))

        return result
