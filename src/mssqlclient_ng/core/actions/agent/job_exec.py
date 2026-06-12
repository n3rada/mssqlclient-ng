# mssqlclient_ng/core/actions/agent/job_exec.py

# Built-in imports
import time
import uuid

# Third party imports
from loguru import logger

# Local library imports
from ..base import Arg, BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext

@ActionFactory.register(
    "job-exec", "Dispatch OS commands asynchronously via SQL Server Agent (CmdExec, PowerShell, TSQL, VBScript). Returns immediately after queuing. Poll output with job-history."
)
class JobExec(BaseAction):
    """
    Execute OS commands via SQL Server Agent by creating a temporary job.
    Supports CmdExec, PowerShell, TSQL, and VBScript subsystems.
    Optionally polls for completion and retrieves output from sysjobhistory.
    """

    VALID_SUBSYSTEMS = ("CmdExec", "PowerShell", "TSQL", "VBScript")

    _command = Arg(position=0, remainder=True, required=True, description="Command to execute")
    _subsystem = Arg(short_name="s", long_name="subsystem", default="PowerShell", description="Subsystem: CmdExec, PowerShell, TSQL, VBScript")
    _wait = Arg(short_name="w", long_name="wait", toggle=True, description="Poll for completion and retrieve output from sysjobhistory")
    _timeout = Arg(short_name="t", long_name="timeout", default=30, description="Polling timeout in seconds")

    def validate_arguments(self, additional_arguments: str = "") -> None:
        super().validate_arguments(additional_arguments)
        self._timeout = int(self._timeout)
        if self._timeout < 1:
            raise ValueError("Timeout must be at least 1 second")
        matched = next(
            (
                s
                for s in self.VALID_SUBSYSTEMS
                if s.lower() == str(self._subsystem).lower()
            ),
            None,
        )
        if not matched:
            raise ValueError(
                f"Invalid subsystem: {self._subsystem}. Valid: {', '.join(self.VALID_SUBSYSTEMS)}"
            )
        self._subsystem = matched

    def execute(self, database_context: DatabaseContext) -> bool | None:
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

        except Exception:
            logger.exception("Agent job failed")
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
