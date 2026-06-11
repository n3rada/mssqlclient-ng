# mssqlclient_ng/core/actions/configmgr/cm_task_sequence.py

"""Display detailed info about a specific ConfigMgr task sequence."""

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter

@ActionFactory.register(
    "cm-tasksequence", "Display detailed information for a specific task sequence including all referenced content."
)
class CMTaskSequence(CMBaseAction):
    """
    Display detailed information about a specific ConfigMgr Task Sequence
    including all referenced content (packages, drivers, applications, OS images, boot images).
    """

    _package_id = Arg(position=0, required=True, description="Task Sequence PackageID")

    def execute(self, database_context: DatabaseContext) -> list | None:
        logger.info(f"Retrieving task sequence details for: {self._package_id}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            query = f"""
SELECT
    ts.PkgID AS PackageID, ts.Name, ts.Description, ts.Version,
    ts.Manufacturer, ts.SourceDate, ts.BootImageID,
    bi.Name AS BootImageName, ts.TS_Type
FROM [{db}].dbo.vSMS_TaskSequencePackage ts
LEFT JOIN [{db}].dbo.v_BootImagePackage bi ON ts.BootImageID = bi.PackageID
WHERE ts.PkgID = '{self._package_id}';"""

            try:
                results = database_context.query_service.execute(query)
                if not results:
                    continue

                logger.success(f"Task Sequence: {results[0].get('Name', 'Unknown')}")
                print(OutputFormatter.convert_list_of_dicts(results))

                # Get referenced content
                ref_query = f"""
SELECT
    ref.PackageID AS ReferencedPackageID,
    ref.ProgramName,
    p.Name AS ContentName,
    p.PackageType,
    p.PkgSourcePath
FROM [{db}].dbo.v_TaskSequenceReferencesInfo ref
LEFT JOIN [{db}].dbo.v_Package p ON ref.PackageID = p.PackageID
WHERE ref.PackageID = '{self._package_id}'
ORDER BY ref.PackageID;"""

                try:
                    refs = database_context.query_service.execute(ref_query)
                    if refs:
                        for r in refs:
                            if "PackageType" in r:
                                r["PackageType"] = CMService.decode_package_type(
                                    r["PackageType"]
                                )
                        logger.success(f"Referenced Content ({len(refs)})")
                        print(OutputFormatter.convert_list_of_dicts(refs))
                except Exception:
                    pass

                return results

            except Exception as ex:
                logger.debug(f"Query failed on {db}: {ex}")

        logger.warning(f"Task sequence '{self._package_id}' not found")
        return None
