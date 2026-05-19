# mssqlclient_ng/core/actions/configmgr/cm_task_sequences.py

"""Enumerate ConfigMgr task sequences."""

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter

@ActionFactory.register(
    "cm-tasksequences",
    "Enumerate ConfigMgr task sequences",
    aliases=["cm-ts"],
)
class CMTaskSequences(CMBaseAction):
    """
    Enumerate ConfigMgr Task Sequences with their properties and referenced content.
    Task Sequences are used for OS deployment and complex automation workflows.
    """

    _name = Arg(short_name="n", long_name="name", default="", description="Filter by name")
    _package_id = Arg(short_name="i", long_name="packageid", default="", description="Filter by PackageID")
    _limit = Arg(long_name="limit", default=25, description="Cap result count")

    def validate_arguments(self, additional_arguments: str = "") -> None:
        super().validate_arguments(additional_arguments)
        self._limit = int(self._limit)

    def execute(self, database_context: DatabaseContext) -> list | None:
        filters = []
        if self._name:
            filters.append(f"name: {self._name}")
        if self._package_id:
            filters.append(f"packageid: {self._package_id}")
        logger.info(
            f"Enumerating ConfigMgr task sequences{' (' + ', '.join(filters) + ')' if filters else ''}"
        )

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            where = "WHERE 1=1"
            if self._name:
                where += f" AND ts.Name LIKE '%{self._name}%'"
            if self._package_id:
                where += f" AND ts.PackageID LIKE '%{self._package_id}%'"

            top = self._build_top_clause(self._limit)

            query = f"""
SELECT {top}
    ts.PackageID, ts.Name, ts.Description, ts.Version,
    ts.SourceDate, ts.BootImageID, ts.TS_Type,
    (SELECT COUNT(*) FROM [{db}].dbo.v_TaskSequenceReferencesInfo ref WHERE ref.PackageID = ts.PkgID) AS ReferencedContentCount
FROM [{db}].dbo.vSMS_TaskSequencePackage ts
{where}
ORDER BY ts.Name;"""

            try:
                results = database_context.query_service.execute(query)
                if results:
                    logger.success(f"Found {len(results)} task sequence(s)")
                    print(OutputFormatter.convert_list_of_dicts(results))
                else:
                    logger.warning("No task sequences found")
            except Exception as ex:
                logger.error(f"Failed to enumerate task sequences: {ex}")

        return results
