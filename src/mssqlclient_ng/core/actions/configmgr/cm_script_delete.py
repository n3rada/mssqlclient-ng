# mssqlclient_ng/core/actions/configmgr/cm_script_delete.py

"""Delete a PowerShell script from ConfigMgr."""

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService

BUILT_IN_CMPIVOT_GUID = "7DC6B6F1-E7F6-43C1-96E0-E1D16BC25C14"

@ActionFactory.register("cm-script-delete", "Remove script from ConfigMgr by GUID to clean up operational artifacts.")
class CMScriptDelete(CMBaseAction):

    _script_guid = Arg(position=0, short_name="g", long_name="guid", required=True, description="Script GUID to delete")

    def validate_arguments(self, additional_arguments: str = "") -> None:
        super().validate_arguments(additional_arguments)
        if self._script_guid.upper() == BUILT_IN_CMPIVOT_GUID:
            raise ValueError("Cannot delete the built-in CMPivot script")

    def execute(self, database_context: DatabaseContext) -> list | None:
        logger.info(f"Deleting ConfigMgr script: {self._script_guid}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            try:
                delete_query = f"DELETE FROM [{db}].dbo.Scripts WHERE ScriptGuid = '{self._script_guid}';"
                rows_affected = database_context.query_service.execute_non_processing(
                    delete_query
                )

                if rows_affected and rows_affected > 0:
                    logger.success(
                        f"Script deleted successfully ({rows_affected} row(s) affected)"
                    )
                else:
                    logger.warning(f"No script found with GUID: {self._script_guid}")
            except Exception as ex:
                logger.error(f"Failed to delete script: {ex}")

        return None
