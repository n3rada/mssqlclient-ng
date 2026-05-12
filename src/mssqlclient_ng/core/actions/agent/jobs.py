# mssqlclient_ng/core/actions/agent/jobs.py

# Built-in imports
from typing import Optional, List, Dict, Any

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


def _check_agent_running(database_context: DatabaseContext) -> bool:
    """Return True if the SQL Server Agent service is running."""
    try:
        query = """
SELECT CASE
    WHEN EXISTS (
        SELECT 1 FROM sys.dm_exec_sessions
        WHERE program_name LIKE 'SQLAgent%'
    ) THEN 'Running' ELSE 'Stopped'
END AS AgentStatus;"""
        result = database_context.query_service.execute_table(query)
        if result:
            status = result[0].get("AgentStatus", "Stopped")
            if status == "Running":
                logger.success("SQL Server Agent is running")
                return True
        logger.error("SQL Server Agent is not running")
        return False
    except Exception as e:
        logger.error(f"Failed to check Agent status: {e}")
        return False


@ActionFactory.register(
    "jobs",
    "List SQL Server Agent jobs with steps, subsystems, owner, and schedule info",
    aliases=["agents"],
)
class Jobs(BaseAction):
    """
    Enumerate SQL Server Agent jobs.

    By default shows one row per job with step count and subsystem summary.
    Use --commands/-c to expand into per-step rows with full command text.
    Use --name/-n to filter by job name (substring match).
    Use --limit/-l to cap result count (default: 25).
    """


    def __init__(self):
        super().__init__()
        self._name: str = ""
        self._show_commands: bool = False
        self._limit: int = 25

    def validate_arguments(self, additional_arguments: str = "") -> None:
        if not additional_arguments or not additional_arguments.strip():
            return

        named, positional = self._parse_action_arguments(additional_arguments)

        # Name filter
        if positional:
            self._name = positional[0]
        self._name = named.get("name", named.get("n", self._name))

        # Show commands flag
        if "commands" in named or "c" in named:
            self._show_commands = True

        # Limit
        limit_str = named.get("limit", named.get("l", ""))
        if limit_str:
            try:
                self._limit = int(limit_str)
            except ValueError:
                raise ValueError(f"Invalid limit value: {limit_str}")

    def execute(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, Any]]]:
        if not _check_agent_running(database_context):
            return None

        filter_msg = f" matching '{self._name}'" if self._name else ""
        logger.info(f"Enumerating SQL Server Agent jobs{filter_msg}")

        where_clause = ""
        if self._name:
            safe_name = self._name.replace("'", "''")
            where_clause = f"WHERE j.name LIKE '%{safe_name}%'"

        top_clause = f"TOP {self._limit}" if self._limit > 0 else ""

        if self._show_commands:
            query = f"""
SELECT {top_clause}
    j.job_id,
    j.name AS JobName,
    SUSER_SNAME(j.owner_sid) AS Owner,
    j.enabled AS Enabled,
    c.name AS Category,
    j.description AS Description,
    j.date_created AS Created,
    j.date_modified AS Modified,
    js.step_id AS StepId,
    js.step_name AS StepName,
    js.subsystem AS Subsystem,
    js.command AS Command,
    js.database_name AS StepDatabase
FROM msdb.dbo.sysjobs j
LEFT JOIN msdb.dbo.sysjobsteps js ON j.job_id = js.job_id
LEFT JOIN msdb.dbo.syscategories c ON j.category_id = c.category_id
{where_clause}
ORDER BY j.name, js.step_id;"""
        else:
            query = f"""
SELECT {top_clause}
    j.job_id,
    j.name AS JobName,
    SUSER_SNAME(j.owner_sid) AS Owner,
    j.enabled AS Enabled,
    c.name AS Category,
    j.description AS Description,
    j.date_created AS Created,
    j.date_modified AS Modified,
    COUNT(js.step_id) AS Steps,
    STUFF((SELECT DISTINCT ', ' + s.subsystem
           FROM msdb.dbo.sysjobsteps s
           WHERE s.job_id = j.job_id
           FOR XML PATH('')), 1, 2, '') AS Subsystems
FROM msdb.dbo.sysjobs j
LEFT JOIN msdb.dbo.sysjobsteps js ON j.job_id = js.job_id
LEFT JOIN msdb.dbo.syscategories c ON j.category_id = c.category_id
{where_clause}
GROUP BY j.job_id, j.name, j.owner_sid, j.enabled, c.name,
         j.description, j.date_created, j.date_modified
ORDER BY j.name;"""

        result = database_context.query_service.execute_table(query)

        if not result:
            logger.info("No SQL Agent jobs found.")
            return None

        print(OutputFormatter.convert_list_of_dicts(result))
        logger.success(f"Found {len(result)} row(s)")
        return result

    def get_arguments(self) -> List[str]:
        return [
            "[job_name]",
            "[-n|--name <filter>]",
            "[-c|--commands]",
            "[-l|--limit <n>]",
        ]
