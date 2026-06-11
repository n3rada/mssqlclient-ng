# mssqlclient_ng/core/actions/configmgr/cm_programs.py

"""Enumerate ConfigMgr programs (legacy package execution configurations)."""

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter

@ActionFactory.register(
    "cm-programs", "Enumerate programs for legacy packages with command lines and decoded execution flags."
)
class CMPrograms(CMBaseAction):
    """
    Enumerate ConfigMgr programs (legacy package execution configurations) with command lines.
    Programs define how packages are executed.
    """

    _package_id = Arg(short_name="p", long_name="package", default="", description="Filter by PackageID")
    _program_name = Arg(short_name="n", long_name="name", default="", description="Filter by program name")
    _command_line = Arg(short_name="c", long_name="commandline", default="", description="Filter by command line")
    _limit = Arg(long_name="limit", default=25, description="Cap result count")

    def validate_arguments(self, additional_arguments: str = "") -> None:
        super().validate_arguments(additional_arguments)
        self._limit = int(self._limit)

    def execute(self, database_context: DatabaseContext) -> list | None:
        filters = []
        if self._package_id:
            filters.append(f"package: {self._package_id}")
        if self._program_name:
            filters.append(f"name: {self._program_name}")
        if self._command_line:
            filters.append(f"commandline: {self._command_line}")
        logger.info(
            f"Enumerating ConfigMgr programs{' (' + ', '.join(filters) + ')' if filters else ''}"
        )

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            where = "WHERE 1=1"
            if self._package_id:
                where += f" AND pr.PackageID LIKE '%{self._package_id}%'"
            if self._program_name:
                where += f" AND pr.ProgramName LIKE '%{self._program_name}%'"
            if self._command_line:
                where += f" AND pr.CommandLine LIKE '%{self._command_line}%'"

            top = self._build_top_clause(self._limit)

            query = f"""
SELECT {top}
    pr.PackageID,
    pk.Name AS PackageName,
    pr.ProgramName,
    pr.CommandLine,
    pr.WorkingDirectory,
    pr.Comment,
    pr.ProgramFlags,
    pr.Duration,
    pr.DiskSpaceRequired
FROM [{db}].dbo.v_Program pr
LEFT JOIN [{db}].dbo.v_Package pk ON pr.PackageID = pk.PackageID
{where}
ORDER BY pr.PackageID, pr.ProgramName;"""

            try:
                results = database_context.query_service.execute(query)
                if results:
                    for row in results:
                        if "ProgramFlags" in row and row["ProgramFlags"] is not None:
                            row["ProgramFlags"] = CMService.decode_program_flags(
                                row["ProgramFlags"]
                            )
                    logger.success(f"Found {len(results)} program(s)")
                    print(OutputFormatter.convert_list_of_dicts(results))
                else:
                    logger.warning("No programs found")
            except Exception as ex:
                logger.error(f"Failed to enumerate programs: {ex}")

        return results
