# mssqlclient_ng/core/actions/configmgr/cm_script.py

"""Display detailed info about a specific ConfigMgr script."""

import base64

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter

@ActionFactory.register(
    "cm-script", "Display detailed information for a specific script including full content and parameters."
)
class CMScript(CMBaseAction):

    _script_guid = Arg(position=0, required=True, description="Script GUID")

    def execute(self, database_context: DatabaseContext) -> list | None:
        logger.info(f"Retrieving ConfigMgr script: {self._script_guid}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            query = f"SELECT * FROM [{db}].dbo.Scripts WHERE ScriptGuid = '{self._script_guid}';"

            try:
                results = database_context.query_service.execute(query)
                if not results:
                    continue

                row = results[0]
                script_name = row.get("ScriptName", "Unknown")
                author = row.get("Author", "Unknown")
                approval_state = row.get("ApprovalState", 0)
                script_version = row.get("ScriptVersion", "")

                approval_str = {0: "Waiting", 1: "Declined", 3: "Approved"}.get(
                    int(approval_state) if approval_state else 0, str(approval_state)
                )

                logger.success(f"{script_name} ({self._script_guid})")
                logger.info(f"  Author: {author}")
                logger.info(f"  Version: {script_version}")
                logger.info(f"  Approval State: {approval_str}")

                # Decode and display script content
                script_blob = row.get("Script")
                if script_blob:
                    try:
                        if isinstance(script_blob, bytes):
                            # Script stored as UTF-16LE with BOM
                            script_content = script_blob.decode(
                                "utf-16-le", errors="replace"
                            )
                        else:
                            script_content = str(script_blob)

                        # Strip BOM if present
                        if script_content.startswith("\ufeff"):
                            script_content = script_content[1:]

                        logger.success("Script Content:")
                        print(script_content)
                    except Exception as ex:
                        logger.debug(f"Failed to decode script content: {ex}")

                # Display parameters if available
                params_def = row.get("ParamsDefinition", "")
                if params_def:
                    logger.info(f"Parameters Definition: {params_def}")

                return results

            except Exception as ex:
                logger.debug(f"Query failed on {db}: {ex}")

        logger.warning(f"Script not found: {self._script_guid}")
        return None
