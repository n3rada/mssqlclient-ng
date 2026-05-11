# mssqlclient_ng/core/actions/configmgr/cm_package.py

"""Display detailed info about a specific ConfigMgr package."""

from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatter import OutputFormatter


@ActionFactory.register("cm-package", "Display details of a specific ConfigMgr package")
class CMPackage(CMBaseAction):
    """Display detailed information about a specific ConfigMgr package including its programs."""


    def __init__(self):
        super().__init__()
        self._package_id: str = ""

    def validate_arguments(self, additional_arguments: str = "", argument_list=None) -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._package_id = self.get_positional_argument(positional, 0, "")
        if not self._package_id:
            raise ValueError("PackageID is required. Usage: cm-package <PackageID>")

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        logger.info(f"Retrieving package details for: {self._package_id}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            query = f"""
SELECT *
FROM [{db}].dbo.v_Package
WHERE PackageID = '{self._package_id}';"""

            try:
                results = database_context.query_service.execute(query)
                if not results:
                    continue

                logger.success(f"Package: {results[0].get('Name', 'Unknown')}")
                print(OutputFormatter.convert_list_of_dicts(results))

                # Get programs for this package
                prog_query = f"""
SELECT ProgramName, CommandLine, WorkingDirectory, Comment, ProgramFlags, Duration
FROM [{db}].dbo.v_Program
WHERE PackageID = '{self._package_id}';"""

                programs = database_context.query_service.execute(prog_query)
                if programs:
                    for prog in programs:
                        if "ProgramFlags" in prog and prog["ProgramFlags"] is not None:
                            prog["ProgramFlags"] = CMService.decode_program_flags(prog["ProgramFlags"])
                    logger.success(f"Programs ({len(programs)})")
                    print(OutputFormatter.convert_list_of_dicts(programs))

                return results

            except Exception as ex:
                logger.debug(f"Query failed on {db}: {ex}")

        logger.warning(f"Package '{self._package_id}' not found")
        return None
