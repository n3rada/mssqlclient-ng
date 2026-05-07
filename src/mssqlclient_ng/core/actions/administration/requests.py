# mssqlclient_ng/core/actions/administration/requests.py

# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register("requests", "Display currently executing SQL requests")
class Requests(BaseAction):
    """
    Retrieves currently executing SQL requests on the SQL Server instance.
    Shows session details, command status, wait types, and blocking information.
    """

    def validate_arguments(self, additional_arguments: str = "") -> None:
        pass

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        logger.info("Currently executing SQL requests")

        query = """
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

        results = database_context.query_service.execute_table(query)

        if results:
            print(OutputFormatter.convert_list_of_dicts(results))
        else:
            logger.info("No other active SQL requests running")

        return results

    def get_arguments(self) -> list[str]:
        return []
