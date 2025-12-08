# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext


@ActionFactory.register("kill", "Kill a specific session or all sessions")
class Kill(BaseAction):
    """
    Terminates SQL Server sessions by session ID or kills all sessions.

    Can target a specific session ID or use 'all' to kill all active sessions.
    """

    def __init__(self):
        super().__init__()
        self._target: str = ""

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the kill target argument.

        Args:
            additional_arguments: Session ID (positive integer) or 'all'

        Raises:
            ValueError: If arguments are invalid.
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError("Please specify a session ID or 'all' as an argument")

        self._target = additional_arguments.strip()

        # Verify target is "all" or a valid integer
        if self._target.lower() != "all":
            try:
                session_id = int(self._target)
                if session_id <= 0:
                    raise ValueError("Session ID must be positive")
            except ValueError:
                raise ValueError(
                    "Invalid argument. Provide a positive session ID or 'all'"
                )

    def execute(self, database_context: DatabaseContext) -> Optional[bool]:
        """
        Executes the kill operation on target session(s).

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            True if successful, False otherwise.
        """
        logger.info(f"Preparing to kill session(s) for target: {self._target}")

        all_sessions_query = """
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

        try:
            # Fetch all running sessions
            logger.info("Fetching all running sessions")
            sessions_table = database_context.query_service.execute_table(
                all_sessions_query
            )

            if not sessions_table:
                logger.warning("No running sessions found")
                return True

            # If specific session ID is provided, validate and kill
            if self._target.lower() != "all":
                target_session_id = int(self._target)
                found_session = None

                for session in sessions_table:
                    if session.get("SessionID") == target_session_id:
                        found_session = session
                        break

                if not found_session:
                    logger.warning(f"Session {self._target} not found or not valid")
                    return False

                # Kill the specific session
                logger.info(f"Killing session {self._target}")
                database_context.query_service.execute(f"KILL {target_session_id};")
                logger.success(f"Session {self._target} killed successfully")
                return True

            # If "all" is specified, loop through all sessions and kill them
            logger.info("Killing all sessions")
            for session in sessions_table:
                session_id = session["SessionID"]
                logger.info(f"Killing session {session_id}")
                database_context.query_service.execute(f"KILL {session_id};")

            logger.success("All sessions killed successfully")
            return True

        except Exception as ex:
            logger.error(f"An error occurred while processing: {ex}")
            return False

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List of argument descriptions.
        """
        return ["<session_id|all>"]
