# mssqlclient_ng/core/actions/agent/job.py

# Built-in imports
import uuid
from typing import Optional, List, Dict, Any

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter
from .jobs import _check_agent_running


@ActionFactory.register(
    "job",
    "Show detailed information about a specific SQL Server Agent job",
)
class Job(BaseAction):
    """
    Display detailed information about a specific SQL Server Agent job.

    Shows job metadata, all steps with commands, schedules, and recent execution history.
    Accepts job name or job_id GUID as identifier.
    """

    def __init__(self):
        super().__init__()
        self._job_identifier: str = ""
        self._history_limit: int = 25

    def validate_arguments(self, additional_arguments: str = "") -> None:
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Job name or job_id is required. Example: job 'Full Backup'"
            )

        named, positional = self._parse_action_arguments(additional_arguments)

        if positional:
            self._job_identifier = positional[0]
        else:
            raise ValueError("Job name or job_id is required.")

        limit_str = named.get("limit", named.get("l", ""))
        if limit_str:
            try:
                self._history_limit = int(limit_str)
            except ValueError:
                raise ValueError(f"Invalid limit value: {limit_str}")

    def execute(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, Any]]]:
        logger.info(f"Retrieving details for job: {self._job_identifier}")

        # Determine if identifier is a GUID or a name
        is_guid = False
        try:
            uuid.UUID(self._job_identifier)
            is_guid = True
        except ValueError:
            pass

        safe_id = self._job_identifier.replace("'", "''")
        if is_guid:
            job_filter = f"j.job_id = '{safe_id}'"
        else:
            job_filter = f"j.name = '{safe_id}'"

        # Job metadata
        job_query = f"""
SELECT
    j.job_id,
    j.name AS JobName,
    SUSER_SNAME(j.owner_sid) AS Owner,
    j.enabled AS Enabled,
    c.name AS Category,
    j.description AS Description,
    j.date_created AS Created,
    j.date_modified AS Modified
FROM msdb.dbo.sysjobs j
LEFT JOIN msdb.dbo.syscategories c ON j.category_id = c.category_id
WHERE {job_filter};"""

        job_info = database_context.query_service.execute_table(job_query)

        if not job_info:
            logger.error(f"Job not found: {self._job_identifier}")
            return None

        job = job_info[0]
        job_id = str(job.get("job_id", ""))
        job_name = job.get("JobName", "")

        logger.info(f"{job_name} ({job_id})")
        logger.info(f"  Owner: {job.get('Owner', '')}")
        logger.info(f"  Enabled: {job.get('Enabled', '')}")
        logger.info(f"  Category: {job.get('Category', '')}")

        description = str(job.get("Description", "") or "")
        if description and description != "No description available.":
            logger.info(f"  Description: {description}")

        logger.info(f"  Created: {job.get('Created', '')}")
        logger.info(f"  Modified: {job.get('Modified', '')}")

        # Job steps
        logger.info("Job Steps:")
        steps_query = f"""
SELECT
    js.step_id AS StepId,
    js.step_name AS StepName,
    js.subsystem AS Subsystem,
    js.database_name AS [Database],
    js.database_user_name AS DatabaseUser,
    js.on_success_action AS OnSuccess,
    js.on_fail_action AS OnFail,
    js.retry_attempts AS RetryAttempts,
    js.output_file_name AS OutputFile,
    p.name AS ProxyName,
    js.command AS Command
FROM msdb.dbo.sysjobsteps js
LEFT JOIN msdb.dbo.sysproxies p ON js.proxy_id = p.proxy_id
WHERE js.job_id = '{job_id}'
ORDER BY js.step_id;"""

        steps = database_context.query_service.execute_table(steps_query)

        if not steps:
            logger.info("  No steps defined")
        else:
            # Display steps without the Command column
            steps_display = [
                {k: v for k, v in s.items() if k != "Command"} for s in steps
            ]
            print(OutputFormatter.convert_list_of_dicts(steps_display))
            logger.success(f"Found {len(steps)} step(s)")

            # Show commands separately
            for step in steps:
                command = str(step.get("Command") or "")
                if command.strip():
                    logger.info(
                        f"Step {step.get('StepId')} Command ({step.get('StepName')}):"
                    )
                    print(command)

        # Schedules
        logger.info("Schedules:")
        schedule_query = f"""
SELECT
    s.name AS ScheduleName,
    s.enabled AS Enabled,
    CASE s.freq_type
        WHEN 1   THEN 'Once'
        WHEN 4   THEN 'Daily'
        WHEN 8   THEN 'Weekly'
        WHEN 16  THEN 'Monthly'
        WHEN 32  THEN 'Monthly (relative)'
        WHEN 64  THEN 'Agent start'
        WHEN 128 THEN 'Idle'
        ELSE CAST(s.freq_type AS VARCHAR)
    END AS Frequency,
    s.freq_interval AS FreqInterval,
    s.active_start_date AS StartDate,
    s.active_start_time AS StartTime,
    s.active_end_date AS EndDate
FROM msdb.dbo.sysjobschedules jsc
JOIN msdb.dbo.sysschedules s ON jsc.schedule_id = s.schedule_id
WHERE jsc.job_id = '{job_id}';"""

        schedules = database_context.query_service.execute_table(schedule_query)

        if not schedules:
            logger.info("  No schedules assigned")
        else:
            print(OutputFormatter.convert_list_of_dicts(schedules))
            logger.success(f"Found {len(schedules)} schedule(s)")

        # Recent history
        top_clause = f"TOP {self._history_limit}" if self._history_limit > 0 else ""
        logger.info(
            f"Recent History (last {self._history_limit})"
            if self._history_limit > 0
            else "Recent History (all)"
        )

        history_query = f"""
SELECT {top_clause}
    h.step_id AS StepId,
    h.step_name AS StepName,
    CASE h.run_status
        WHEN 0 THEN 'Failed'
        WHEN 1 THEN 'Succeeded'
        WHEN 2 THEN 'Retry'
        WHEN 3 THEN 'Cancelled'
        WHEN 4 THEN 'Running'
        ELSE CAST(h.run_status AS VARCHAR)
    END AS RunStatus,
    h.run_date AS RunDate,
    h.run_time AS RunTime,
    h.run_duration AS Duration,
    h.retries_attempted AS Retries,
    h.message AS Message
FROM msdb.dbo.sysjobhistory h
WHERE h.job_id = '{job_id}'
ORDER BY h.run_date DESC, h.run_time DESC;"""

        history = database_context.query_service.execute_table(history_query)

        if not history:
            logger.info("  No execution history")
        else:
            print(OutputFormatter.convert_list_of_dicts(history))
            logger.success(f"Found {len(history)} history record(s)")

        return job_info

    def get_arguments(self) -> List[str]:
        return ["<job_name|job_id>", "[-l|--limit <n>]"]
