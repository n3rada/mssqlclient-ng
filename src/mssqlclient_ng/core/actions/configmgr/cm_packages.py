# mssqlclient_ng/core/actions/configmgr/cm_packages.py

"""Enumerate ConfigMgr packages."""

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter

@ActionFactory.register(
    "cm-packages", "Enumerate ConfigMgr packages (legacy deployment)"
)
class CMPackages(CMBaseAction):
    """
    Enumerate ConfigMgr packages with their properties, source locations, and program details.
    Packages are the legacy deployment model.
    """

    _name = Arg(short_name="n", long_name="name", default="", description="Filter by package name")
    _source_path = Arg(short_name="s", long_name="source", default="", description="Filter by source path")
    _manufacturer = Arg(short_name="m", long_name="manufacturer", default="", description="Filter by manufacturer")
    _limit = Arg(long_name="limit", default=25, description="Cap result count")

    def __init__(self):
        super().__init__()
        self._name: str = ""
        self._source_path: str = ""
        self._manufacturer: str = ""
        self._limit: int = 25

    def validate_arguments(self, additional_arguments: str = "") -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._name = named.get("name", named.get("n", ""))
        self._source_path = named.get("source", named.get("s", ""))
        self._manufacturer = named.get("manufacturer", named.get("m", ""))
        self._limit = int(named.get("limit", "25"))

    def execute(self, database_context: DatabaseContext) -> list | None:
        filters = []
        if self._name:
            filters.append(f"name: {self._name}")
        if self._source_path:
            filters.append(f"source: {self._source_path}")
        logger.info(
            f"Enumerating ConfigMgr packages{' (' + ', '.join(filters) + ')' if filters else ''}"
        )

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            where = "WHERE 1=1"
            if self._name:
                where += f" AND p.Name LIKE '%{self._name}%'"
            if self._source_path:
                where += f" AND p.PkgSourcePath LIKE '%{self._source_path}%'"
            if self._manufacturer:
                where += f" AND p.Manufacturer LIKE '%{self._manufacturer}%'"

            top = self._build_top_clause(self._limit)

            query = f"""
SELECT {top}
    p.PackageID,
    p.Name,
    p.Description,
    p.Manufacturer,
    p.Version,
    p.PackageType,
    p.PkgSourcePath,
    p.LastRefreshTime,
    (SELECT COUNT(*) FROM [{db}].dbo.v_Program pr WHERE pr.PackageID = p.PackageID) AS ProgramCount
FROM [{db}].dbo.v_Package p
{where}
ORDER BY p.Name;"""

            try:
                results = database_context.query_service.execute(query)
                if results:
                    for row in results:
                        if "PackageType" in row:
                            row["PackageType"] = CMService.decode_package_type(
                                row["PackageType"]
                            )
                    logger.success(f"Found {len(results)} package(s)")
                    print(OutputFormatter.convert_list_of_dicts(results))
                else:
                    logger.warning("No packages found")
            except Exception as ex:
                logger.error(f"Failed to enumerate packages: {ex}")

        return results
