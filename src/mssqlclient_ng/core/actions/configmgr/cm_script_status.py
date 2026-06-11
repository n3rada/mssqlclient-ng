# mssqlclient_ng/core/actions/configmgr/cm_script_status.py

"""Monitor ConfigMgr script execution status."""

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter

@ActionFactory.register("cm-script-status", "Monitor script execution status and retrieve output from target devices by Task ID.")
class CMScriptStatus(CMBaseAction):
    """
    Monitor execution status and retrieve output from scripts run via cm-script-run.
    Shows task state (Pending/Success/Failed), execution time, script output, and errors.
    """

    _task_id = Arg(position=0, short_name="t", long_name="taskid", required=True, description="Task ID returned by cm-script-run")

    def __init__(self):
        super().__init__()
        self._task_id: str = ""

    def validate_arguments(self, additional_arguments: str = "") -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._task_id = named.get(
            "taskid", named.get("t", "")
        ) or self.get_positional_argument(positional, 0, "")
        if not self._task_id:
            raise ValueError("Task ID is required. Usage: cm-script-status <task_id>")

    def execute(self, database_context: DatabaseContext) -> list | None:
        logger.info(f"Checking status for Task ID: {self._task_id}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            query = f"""
SELECT
    ses.TaskID,
    ses.ScriptExecutionState,
    ses.ScriptExitCode,
    ses.ScriptOutput,
    ses.LastUpdateTime,
    s.ScriptName,
    s.ScriptGuid,
    sys.Name0 AS DeviceName,
    sys.ResourceID
FROM [{db}].dbo.ScriptsExecutionStatus ses
LEFT JOIN [{db}].dbo.Scripts s ON ses.ScriptGuid = s.ScriptGuid
LEFT JOIN [{db}].dbo.v_R_System sys ON ses.ResourceID = sys.ResourceID
WHERE ses.TaskID = {self._task_id};"""

            try:
                results = database_context.query_service.execute(query)
                if not results:
                    logger.warning(
                        f"No execution record found for Task ID: {self._task_id}"
                    )
                    logger.info("Task may still be in queue or device is offline")
                    continue

                row = results[0]
                script_name = row.get("ScriptName", "Unknown")
                device_name = row.get("DeviceName", "Unknown")
                execution_state = row.get("ScriptExecutionState", "Unknown")
                exit_code = row.get("ScriptExitCode", -1)
                script_output = row.get("ScriptOutput", "")

                logger.success("Task Status Found")
                logger.info(f"  Script: {script_name}")
                logger.info(f"  Device: {device_name}")
                logger.info(f"  State: {execution_state}")
                logger.info(f"  Exit Code: {exit_code}")

                if script_output:
                    logger.success("Script Output:")
                    print(script_output)

                return results

            except Exception as ex:
                logger.error(f"Failed to query status: {ex}")

        return None
