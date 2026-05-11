# mssqlclient_ng/core/actions/agent/job_exec.py

# Built-in imports
import time
import uuid
from typing import Optional

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext


@ActionFactory.register(
    "job-exec", "Execute OS commands via SQL Server Agent temporary job"
)
class JobExec(BaseAction):
    """
    Execute OS commands via SQL Server Agent by creating a temporary job.
    Supports CmdExec, PowerShell, TSQL, and VBScript subsystems.
    Optionally polls for completion and retrieves output from sysjobhistory.
    """

    VALID_SUBSYSTEMS = ("CmdExec", "PowerShell", "TSQL", "VBScript")

    def __init__(self):
        super().__init__()
        self._command: str = ""
        self._subsystem: str = "PowerShell"
        self._wait: bool = False
        self._timeout: int = 30

    def validate_arguments(
        self, additional_arguments: str = "", argument_list=None
    ) -> None:
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Missing command to execute. "
                "Usage: job-exec <command> [--subsystem CmdExec|PowerShell|TSQL|VBScript] [--wait] [--timeout N]"
            )

        named_args, positional = self._parse_action_arguments(additional_arguments)

        if not positional:
            raise ValueError("Missing command to execute.")

        self._command = " ".join(positional)

        if "subsystem" in named_args or "s" in named_args:
            sub = named_args.get("subsystem", named_args.get("s", "PowerShell"))
            # Case-insensitive match
            matched = next(
                (s for s in self.VALID_SUBSYSTEMS if s.lower() == sub.lower()), None
            )
            if not matched:
                raise ValueError(
                    f"Invalid subsystem: {sub}. Valid: {', '.join(self.VALID_SUBSYSTEMS)}"
                )
            self._subsystem = matched

        if "wait" in named_args or "w" in named_args:
            self._wait = True

        if "timeout" in named_args or "t" in named_args:
            t = named_args.get("timeout", named_args.get("t", "30"))
            self._timeout = int(t)
            if self._timeout < 1:
                raise ValueError("Timeout must be at least 1 second")

    def execute(self, database_context: DatabaseContext) -> Optional[bool]:
        # Check agent is running
        result = database_context.query_service.execute_table(
            "SELECT 1 FROM master.dbo.sysprocesses WHERE program_name LIKE 'SQLAgent%'"
        )
        if not result:
            logger.error("SQL Server Agent is not running")
            return None

        logger.info(f"Subsystem: {self._subsystem}")

        job_name = f"SQLMaint_{uuid.uuid4().hex[:8]}"
        step_name = f"Step_{uuid.uuid4().hex[:8]}"
        escaped_cmd = self._command.replace("'", "''")

        try:
            # Create job
            database_context.query_service.execute_non_processing(
                f"EXEC msdb.dbo.sp_add_job "
                f"@job_name = '{job_name}', "
                f"@enabled = 1, "
                f"@description = 'Routine maintenance task';"
            )
            logger.success(f"Job '{job_name}' created")

            # Add job step
            database_context.query_service.execute_non_processing(
                f"EXEC msdb.dbo.sp_add_jobstep "
                f"@job_name = '{job_name}', "
                f"@step_name = '{step_name}', "
                f"@subsystem = '{self._subsystem}', "
                f"@command = '{escaped_cmd}', "
                f"@retry_attempts = 0, "
                f"@retry_interval = 0;"
            )
            logger.success(f"Job step added [{self._subsystem}]")

            # Assign to local server
            database_context.query_service.execute_non_processing(
                f"EXEC msdb.dbo.sp_add_jobserver @job_name = '{job_name}', @server_name = '(local)';"
            )

            # Start job
            logger.info(f"Starting job '{job_name}'")
            database_context.query_service.execute_non_processing(
                f"EXEC msdb.dbo.sp_start_job @job_name = '{job_name}';"
            )
            logger.success("Job started")

            if self._wait:
                self._poll_job_completion(database_context, job_name)
            else:
                logger.warning("Asynchronous execution")
                logger.info("Use --wait to poll for completion and retrieve output")

            # Cleanup
            logger.info(f"Cleaning up job '{job_name}'")
            database_context.query_service.execute_non_processing(
                f"EXEC msdb.dbo.sp_delete_job @job_name = '{job_name}';"
            )
            logger.success("Job cleaned up")
            return True

        except Exception as ex:
            logger.error(f"Agent job failed: {ex}")
            self._cleanup_job(database_context, job_name)
            return False

    def _poll_job_completion(
        self, database_context: DatabaseContext, job_name: str
    ) -> None:
        """Poll sysjobhistory until job completes or timeout is reached."""
        deadline = time.time() + self._timeout

        while time.time() < deadline:
            time.sleep(1)

            query = (
                f"SELECT TOP 1 run_status, message "
                f"FROM msdb.dbo.sysjobhistory "
                f"WHERE job_id = (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = '{job_name}') "
                f"AND step_id = 0 "
                f"ORDER BY run_date DESC, run_time DESC;"
            )

            history = database_context.query_service.execute_table(query)
            if not history:
                continue

            run_status = int(history[0].get("run_status", -1))
            message = history[0].get("message", "")

            if run_status == 1:
                logger.success("Job completed successfully")
                if message:
                    logger.info(f"Output: {message}")
                return
            elif run_status == 0:
                logger.error("Job failed")
                if message:
                    logger.error(message)
                return
            elif run_status == 3:
                logger.warning("Job was cancelled")
                return

        logger.warning(f"Timeout reached ({self._timeout}s): job may still be running")

    def _cleanup_job(self, database_context: DatabaseContext, job_name: str) -> None:
        """Best-effort job cleanup."""
        try:
            database_context.query_service.execute_non_processing(
                f"EXEC msdb.dbo.sp_delete_job @job_name = '{job_name}';"
            )
        except Exception:
            pass

    def get_arguments(self) -> list:
        return [
            "<command>",
            "[--subsystem CmdExec|PowerShell|TSQL|VBScript]",
            "[--wait]",
            "[--timeout N]",
        ]
