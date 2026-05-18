# mssqlclient_ng/core/actions/agent/job_history.py

# Built-in imports
from typing import Optional, List, Dict, Any

# Third party imports
from loguru import logger

# Local library imports
from ..base import Arg, BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "job-history",
    "Display SQL Server Agent job execution history",
)
class JobHistory(BaseAction):
    """
    Show SQL Server Agent job execution history from msdb.dbo.sysjobhistory.

    Optionally filter by job name (substring) and limit the number of rows.
    Use --failed/-f to show only failed runs.
    """

    _name = Arg(position=0, short_name="n", long_name="name", default="", description="Filter by job name (substring match)")
    _failed_only = Arg(short_name="f", long_name="failed", toggle=True, description="Show only failed runs")
    _limit = Arg(short_name="l", long_name="limit", default=25, description="Cap result count")

    def validate_arguments(self, additional_arguments: str = "") -> None:
        super().validate_arguments(additional_arguments)
        self._limit = int(self._limit)

    def execute(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, Any]]]:
        filter_msg = f" for '{self._name}'" if self._name else ""
        failed_msg = " (failed only)" if self._failed_only else ""
        logger.info(f"Retrieving Agent job history{filter_msg}{failed_msg}")

        where_parts = ["1=1"]

        if self._name:
            safe_name = self._name.replace("'", "''")
            where_parts.append(f"j.name LIKE '%{safe_name}%'")

        if self._failed_only:
            where_parts.append("h.run_status = 0")

        where_clause = "WHERE " + " AND ".join(where_parts)
        top_clause = f"TOP {self._limit}" if self._limit > 0 else ""

        query = f"""
SELECT {top_clause}
    j.name AS JobName,
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
    h.sql_severity AS Severity,
    h.retries_attempted AS Retries,
    h.message AS Message
FROM msdb.dbo.sysjobhistory h
JOIN msdb.dbo.sysjobs j ON h.job_id = j.job_id
{where_clause}
ORDER BY h.run_date DESC, h.run_time DESC;"""

        result = database_context.query_service.execute_table(query)

        if not result:
            logger.info("No execution history found.")
            return None

        print(OutputFormatter.convert_list_of_dicts(result))
        logger.success(f"Found {len(result)} history record(s)")
        return result
