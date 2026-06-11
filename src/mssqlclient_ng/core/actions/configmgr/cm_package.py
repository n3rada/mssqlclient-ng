# mssqlclient_ng/core/actions/configmgr/cm_package.py

"""Display detailed info about a specific ConfigMgr package."""

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter

@ActionFactory.register("cm-package", "Display comprehensive information about a specific package including programs and deployments.")
class CMPackage(CMBaseAction):
    """Display detailed information about a specific ConfigMgr package including its programs."""

    _package_id = Arg(position=0, required=True, description="Package ID to inspect")

    def execute(self, database_context: DatabaseContext) -> list | None:
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
                            prog["ProgramFlags"] = CMService.decode_program_flags(
                                prog["ProgramFlags"]
                            )
                    logger.success(f"Programs ({len(programs)})")
                    print(OutputFormatter.convert_list_of_dicts(programs))

                return results

            except Exception as ex:
                logger.debug(f"Query failed on {db}: {ex}")

        logger.warning(f"Package '{self._package_id}' not found")
        return None
