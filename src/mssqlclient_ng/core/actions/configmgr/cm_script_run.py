# mssqlclient_ng/core/actions/configmgr/cm_script_run.py

"""Execute a PowerShell script on a target device through ConfigMgr BGB channel."""

import base64
import uuid

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService

@ActionFactory.register(
    "cm-script-run", "Execute PowerShell script on target device via BGB notification channel (requires ResourceID and script GUID)."
)
class CMScriptRun(CMBaseAction):

    _resource_id = Arg(short_name="r", long_name="resourceid", required=True, description="Target device ResourceID (from cm-devices)")
    _script_guid = Arg(short_name="g", long_name="scriptguid", required=True, description="Script GUID (from cm-scripts)")

    def __init__(self):
        super().__init__()
        self._resource_id: str = ""
        self._script_guid: str = ""

    def validate_arguments(self, additional_arguments: str = "") -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._resource_id = named.get("resourceid", named.get("r", ""))
        self._script_guid = named.get("scriptguid", named.get("g", ""))
        if not self._resource_id:
            raise ValueError("--resourceid is required")
        if not self._script_guid:
            raise ValueError("--scriptguid is required")

    def execute(self, database_context: DatabaseContext) -> list | None:
        logger.info(f"Executing ConfigMgr script on ResourceID: {self._resource_id}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            try:
                # Step 1: Verify script exists
                script_query = f"""
SELECT ScriptHash, ScriptVersion, ScriptName
FROM [{db}].dbo.Scripts
WHERE ScriptGuid = '{self._script_guid}';"""

                script_info = database_context.query_service.execute(script_query)
                if not script_info:
                    logger.error(f"Script not found: {self._script_guid}")
                    continue

                script_hash = script_info[0].get("ScriptHash", "")
                script_version = script_info[0].get("ScriptVersion", 1)
                script_name = script_info[0].get("ScriptName", "")
                logger.info(f"  Script: {script_name} (v{script_version})")

                # Step 2: Check target device
                device_query = f"""
SELECT Name0, OnlineStatus
FROM [{db}].dbo.v_R_System sys
LEFT JOIN [{db}].dbo.BGB_ResStatus bgb ON sys.ResourceID = bgb.ResourceID
WHERE sys.ResourceID = {self._resource_id};"""

                device_info = database_context.query_service.execute(device_query)
                if not device_info:
                    logger.error(f"Device not found: ResourceID {self._resource_id}")
                    continue

                device_name = device_info[0].get("Name0", "Unknown")
                logger.info(f"  Target: {device_name}")

                # Step 3: Create TaskParam XML
                task_param = (
                    f"<ScriptContent ScriptGuid='{self._script_guid}'>"
                    f"<ScriptVersion>{script_version}</ScriptVersion>"
                    f"<ScriptType>0</ScriptType>"
                    f"<ScriptHash ScriptHashAlg='SHA256'>{script_hash}</ScriptHash>"
                    f"<ScriptParameters></ScriptParameters>"
                    f"<ParameterGroupHash ParameterHashAlg='SHA256'></ParameterGroupHash>"
                    f"</ScriptContent>"
                )
                task_param_b64 = base64.b64encode(task_param.encode("utf-8")).decode(
                    "ascii"
                )
                task_guid = str(uuid.uuid4()).upper()

                # Step 4: Insert BGB_Task
                insert_task = f"""
INSERT INTO [{db}].dbo.BGB_Task
(TemplateID, CreateTime, Signature, GUID, Param)
VALUES (15, '', NULL, '{task_guid}', '{task_param_b64}');"""

                database_context.query_service.execute_non_processing(insert_task)
                logger.success("Created BGB_Task entry")
                logger.info(f"  Task GUID: {task_guid}")

                # Step 5: Get TaskID
                get_task_id = f"SELECT TaskID FROM [{db}].dbo.BGB_Task WHERE GUID = '{task_guid}';"
                task_result = database_context.query_service.execute(get_task_id)
                if not task_result:
                    logger.error("Failed to retrieve TaskID")
                    continue

                task_id = task_result[0]["TaskID"]
                logger.info(f"  Task ID: {task_id}")

                # Step 6: Insert BGB_ResTask (triggers push notification)
                insert_res_task = f"""
INSERT INTO [{db}].dbo.BGB_ResTask
(ResourceID, TemplateID, TaskID, Param)
VALUES ({self._resource_id}, 15, {task_id}, '{task_param_b64}');"""

                database_context.query_service.execute_non_processing(insert_res_task)
                logger.success("Script execution queued successfully")
                logger.info(f"  Use 'cm-script-status {task_id}' to monitor execution")

                return [
                    {"TaskID": task_id, "TaskGUID": task_guid, "Device": device_name}
                ]

            except Exception as ex:
                logger.error(f"Failed to execute script: {ex}")

        return None
