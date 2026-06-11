# mssqlclient_ng/core/actions/configmgr/cm_scripts.py

"""Enumerate ConfigMgr PowerShell scripts."""

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter

@ActionFactory.register("cm-scripts", "Enumerate PowerShell scripts with metadata overview (excludes script content).")
class CMScripts(CMBaseAction):
    """
    Enumerate PowerShell scripts stored in ConfigMgr.
    Use 'cm-script <GUID>' to view full details and script content.
    """

    _name = Arg(position=0, short_name="n", long_name="name", default="", description="Filter by script name")

    def execute(self, database_context: DatabaseContext) -> list | None:
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
                    logger.info(
                        "Use 'cm-script <GUID>' to view full content and details"
                    )
                else:
                    logger.warning("No scripts found")
            except Exception as ex:
                logger.error(f"Failed to enumerate scripts: {ex}")

        return results
