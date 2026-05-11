# mssqlclient_ng/core/actions/configmgr/cm_programs.py

"""Enumerate ConfigMgr programs (legacy package execution configurations)."""

from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatter import OutputFormatter


@ActionFactory.register("cm-programs", "Enumerate ConfigMgr programs with command lines")
class CMPrograms(CMBaseAction):
    """
    Enumerate ConfigMgr programs (legacy package execution configurations) with command lines.
    Programs define how packages are executed.
    """


    def __init__(self):
        super().__init__()
        self._package_id: str = ""
        self._program_name: str = ""
        self._command_line: str = ""
        self._limit: int = 25

    def validate_arguments(self, additional_arguments: str = "", argument_list=None) -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._package_id = named.get("package", named.get("p", ""))
        self._program_name = named.get("name", named.get("n", ""))
        self._command_line = named.get("commandline", named.get("c", ""))
        self._limit = int(named.get("limit", "25"))

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        filters = []
        if self._package_id:
            filters.append(f"package: {self._package_id}")
        if self._program_name:
            filters.append(f"name: {self._program_name}")
        if self._command_line:
            filters.append(f"commandline: {self._command_line}")
        logger.info(f"Enumerating ConfigMgr programs{' (' + ', '.join(filters) + ')' if filters else ''}")

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
                            row["ProgramFlags"] = CMService.decode_program_flags(row["ProgramFlags"])
                    logger.success(f"Found {len(results)} program(s)")
                    print(OutputFormatter.convert_list_of_dicts(results))
                else:
                    logger.warning("No programs found")
            except Exception as ex:
                logger.error(f"Failed to enumerate programs: {ex}")

        return None
