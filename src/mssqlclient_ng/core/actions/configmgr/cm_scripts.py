# mssqlclient_ng/core/actions/configmgr/cm_scripts.py

"""Enumerate ConfigMgr PowerShell scripts."""

from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter


@ActionFactory.register("cm-scripts", "Enumerate ConfigMgr PowerShell scripts")
class CMScripts(CMBaseAction):
    """
    Enumerate PowerShell scripts stored in ConfigMgr.
    Use 'cm-script <GUID>' to view full details and script content.
    """


    def __init__(self):
        super().__init__()
        self._name: str = ""

    def validate_arguments(self, additional_arguments: str = "") -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._name = named.get("name", named.get("n", "")) or self.get_positional_argument(positional, 0, "")

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        logger.info("Enumerating ConfigMgr scripts")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            # Exclude built-in CMPivot script
            where = "WHERE ScriptGuid != '7DC6B6F1-E7F6-43C1-96E0-E1D16BC25C14'"
            if self._name:
                where += f" AND ScriptName LIKE '%{self._name}%'"

            query = f"""
SELECT
    ScriptGuid, ScriptVersion, ScriptName, Author, ScriptType,
    ApprovalState, Approver, ScriptHash, LastUpdateTime,
    Comment, ScriptDescription, Timeout
FROM [{db}].dbo.Scripts
{where}
ORDER BY LastUpdateTime DESC;"""

            try:
                results = database_context.query_service.execute(query)
                if results:
                    logger.success(f"Found {len(results)} script(s)")
                    print(OutputFormatter.convert_list_of_dicts(results))
                    logger.info("Use 'cm-script <GUID>' to view full content and details")
                else:
                    logger.warning("No scripts found")
            except Exception as ex:
                logger.error(f"Failed to enumerate scripts: {ex}")

        return None
