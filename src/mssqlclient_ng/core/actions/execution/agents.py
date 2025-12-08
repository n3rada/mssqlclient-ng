"""
Agents action for managing SQL Server Agent jobs (list, execute commands).
"""

import time
from enum import Enum
from typing import Optional, List, Dict, Any
from loguru import logger

from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils import common
from ...utils.formatters import OutputFormatter


class ActionMode(Enum):
    """Action modes for agent operations."""

    STATUS = "status"
    EXEC = "exec"


class SubSystemMode(Enum):
    """Subsystem types for agent job execution."""

    CMD = "CmdExec"
    POWERSHELL = "PowerShell"
    TSQL = "TSQL"
    VBSCRIPT = "VBScript"


@ActionFactory.register("agents", "Manage SQL Server Agent jobs (list, execute)")
class Agents(BaseAction):
    """
    Execute SQL Server Agent actions (list jobs, execute commands).

    SQL Server Agent is a Microsoft Windows service that executes scheduled
    administrative tasks (jobs). This action can list existing jobs or create
    temporary jobs to execute commands.
    """

    def __init__(self):
        super().__init__()
        self._action: ActionMode = ActionMode.STATUS
        self._command: Optional[str] = None
        self._subsystem: SubSystemMode = SubSystemMode.POWERSHELL

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate and parse arguments for the agents action.

        Args:
            additional_arguments: Space-separated arguments:
                - action: 'status' or 'exec' (default: status)
                - command: Command to execute (required for exec mode)
                - subsystem: 'cmd', 'powershell', 'tsql', 'vbscript' (default: powershell)

        Raises:
            ValueError: If arguments are invalid
        """
        if not additional_arguments or not additional_arguments.strip():
            # Default to status mode
            return

        parts = additional_arguments.split(maxsplit=2)

        # Parse action mode
        action_str = parts[0].lower()
        try:
            self._action = ActionMode(action_str)
        except ValueError:
            valid_actions = ", ".join([mode.value for mode in ActionMode])
            raise ValueError(
                f"Invalid action: {action_str}. Valid actions are: {valid_actions}"
            )

        # Parse exec mode arguments
        if self._action == ActionMode.EXEC:
            if len(parts) < 2:
                raise ValueError(
                    "Missing command to execute. Example: agents exec 'whoami'"
                )

            self._command = parts[1].strip()

            # Optional: Parse subsystem
            if len(parts) > 2:
                subsystem_str = parts[2].lower()
                subsystem_map = {
                    "cmd": SubSystemMode.CMD,
                    "powershell": SubSystemMode.POWERSHELL,
                    "tsql": SubSystemMode.TSQL,
                    "vbscript": SubSystemMode.VBSCRIPT,
                }

                if subsystem_str not in subsystem_map:
                    valid_subsystems = ", ".join(subsystem_map.keys())
                    raise ValueError(
                        f"Invalid subsystem: {subsystem_str}. Valid subsystems are: {valid_subsystems}"
                    )

                self._subsystem = subsystem_map[subsystem_str]

    def execute(self, database_context: DatabaseContext) -> Optional[Any]:
        """
        Execute the agents action.

        Args:
            database_context: The database context

        Returns:
            Result of the action (job list or execution status)
        """
        logger.info(f"Executing {self._action.value} mode")

        if self._action == ActionMode.STATUS:
            return self._list_agent_jobs(database_context)
        elif self._action == ActionMode.EXEC:
            return self._execute_agent_job(database_context)

        logger.error("Unknown execution mode")
        return None

    def _agent_status(self, database_context: DatabaseContext) -> bool:
        """
        Check if SQL Server Agent is running.

        Args:
            database_context: The database context

        Returns:
            True if Agent is running, False otherwise
        """
        try:
            query = """
                IF EXISTS (SELECT 1 FROM master.dbo.sysprocesses WHERE program_name LIKE 'SQLAgent%')
                    SELECT 'Running' AS AgentStatus
                ELSE
                    SELECT 'Stopped' AS AgentStatus;
            """

            result = database_context.query_service.execute_table(query)
            if result and len(result) > 0:
                status = result[0].get("AgentStatus", "Stopped")

                if status == "Running":
                    logger.success("SQL Server Agent is running")
                    return True
                else:
                    logger.error("SQL Server Agent is not running")
                    logger.info(
                        "Agent jobs require the SQL Server Agent service to be active."
                    )
                    return False
            else:
                logger.error("Failed to determine Agent status")
                return False

        except Exception as e:
            logger.error(f"Failed to check Agent status: {e}")
            return False

    def _list_agent_jobs(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, Any]]]:
        """
        List SQL Server Agent jobs.

        Args:
            database_context: The database context

        Returns:
            List of job dictionaries
        """
        if not self._agent_status(database_context):
            return None

        logger.info("Retrieving SQL Server Agent Jobs...")

        query = """
            SELECT
                job_id,
                name,
                enabled,
                date_created,
                date_modified
            FROM msdb.dbo.sysjobs
            ORDER BY date_created;
        """

        try:
            jobs = database_context.query_service.execute_table(query)

            if not jobs or len(jobs) == 0:
                logger.info("No SQL Agent jobs found")
                return None

            logger.success(f"Found {len(jobs)} SQL Agent job(s)")
            print()
            print(OutputFormatter.convert_list_of_dicts(jobs))

            return jobs

        except Exception as e:
            logger.error(f"Failed to retrieve agent jobs: {e}")
            return None

    def _execute_agent_job(self, database_context: DatabaseContext) -> Optional[bool]:
        """
        Execute a command using SQL Server Agent.

        Args:
            database_context: The database context

        Returns:
            True if successful, False otherwise
        """
        if not self._agent_status(database_context):
            return False

        logger.info(
            f"Creating and executing agent job with {self._subsystem.value} subsystem..."
        )

        # Generate unique job and step names
        job_name = f"AZ_Job_{common.generate_random_string(8)}"
        step_name = f"AZ_Step_{common.generate_random_string(8)}"

        try:
            # Create job
            create_job_query = f"""
                EXEC msdb.dbo.sp_add_job
                    @job_name = '{job_name}',
                    @enabled = 1,
                    @description = 'mssqlclient-ng temporary job';
            """

            database_context.query_service.execute_non_processing(create_job_query)
            logger.success(f"Job '{job_name}' created")

            # Escape single quotes in command
            escaped_command = self._command.replace("'", "''")

            # Add job step
            add_step_query = f"""
                EXEC msdb.dbo.sp_add_jobstep
                    @job_name = '{job_name}',
                    @step_name = '{step_name}',
                    @subsystem = '{self._subsystem.value}',
                    @command = '{escaped_command}',
                    @retry_attempts = 0,
                    @retry_interval = 0;
            """

            database_context.query_service.execute_non_processing(add_step_query)
            logger.success(
                f"Job step '{step_name}' added with {self._subsystem.value} subsystem"
            )

            # Add job server
            add_server_query = (
                f"EXEC msdb.dbo.sp_add_jobserver @job_name = '{job_name}', "
                f"@server_name = '(local)';"
            )
            database_context.query_service.execute_non_processing(add_server_query)

            # Start job
            logger.info(f"Starting job '{job_name}'...")
            start_job_query = f"EXEC msdb.dbo.sp_start_job @job_name = '{job_name}';"
            database_context.query_service.execute_non_processing(start_job_query)

            logger.success(f"Job '{job_name}' started successfully")
            logger.warning(
                "Note: This is an asynchronous execution. Check job history for output."
            )

            # Wait for job to execute
            time.sleep(2)

            # Clean up
            logger.info(f"Cleaning up job '{job_name}'...")
            delete_job_query = f"EXEC msdb.dbo.sp_delete_job @job_name = '{job_name}';"
            database_context.query_service.execute_non_processing(delete_job_query)
            logger.success("Job cleaned up")

            return True

        except Exception as e:
            logger.error(f"Failed to execute agent job: {e}")

            # Try to clean up
            try:
                delete_job_query = (
                    f"EXEC msdb.dbo.sp_delete_job @job_name = '{job_name}';"
                )
                database_context.query_service.execute_non_processing(delete_job_query)
            except Exception:
                # Ignore cleanup errors
                pass

            return False

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return [
            "Action mode: 'status' or 'exec' (default: status)",
            "Command to execute (required for exec mode)",
            "Subsystem: 'cmd', 'powershell', 'tsql', 'vbscript' (default: powershell)",
        ]
