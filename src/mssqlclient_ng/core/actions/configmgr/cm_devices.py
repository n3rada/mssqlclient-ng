# mssqlclient_ng/core/actions/configmgr/cm_devices.py

"""Enumerate ConfigMgr-managed devices."""

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter

@ActionFactory.register("cm-devices", "Enumerate managed devices with filtering by attributes for device discovery and inventory queries.")
class CMDevices(CMBaseAction):
    """
    Enumerate ConfigMgr-managed devices with filtering by name, domain, user, IP, or collection.
    """

    _name = Arg(position=0, short_name="n", long_name="name", default="", description="Filter by device name")
    _domain = Arg(short_name="d", long_name="domain", default="", description="Filter by domain")
    _username = Arg(short_name="u", long_name="user", default="", description="Filter by last logged-in user")
    _ip = Arg(short_name="i", long_name="ip", default="", description="Filter by IP address")
    _collection = Arg(short_name="c", long_name="collection", default="", description="Filter by collection name")
    _online_only = Arg(short_name="o", long_name="online", toggle=True, description="Show only online devices")
    _client_only = Arg(long_name="client-only", toggle=True, description="Show only devices with CM client")
    _limit = Arg(long_name="limit", default=25, description="Cap result count")
    _count_only = Arg(long_name="count", toggle=True, description="Return only device count")

    def __init__(self):
        super().__init__()
        self._name: str = ""
        self._domain: str = ""
        self._username: str = ""
        self._ip: str = ""
        self._collection: str = ""
        self._online_only: bool = False
        self._client_only: bool = False
        self._limit: int = 25
        self._count_only: bool = False

    def validate_arguments(self, additional_arguments: str = "") -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._name = named.get(
            "name", named.get("n", "")
        ) or self.get_positional_argument(positional, 0, "")
        self._domain = named.get("domain", named.get("d", ""))
        self._username = named.get("user", named.get("u", ""))
        self._ip = named.get("ip", named.get("i", ""))
        self._collection = named.get("collection", named.get("c", ""))
        self._online_only = "online" in named or "o" in named
        self._client_only = "client-only" in named
        self._limit = int(named.get("limit", "25"))
        self._count_only = "count" in named

    def execute(self, database_context: DatabaseContext) -> list | None:
        filters = []
        if self._name:
            filters.append(f"name: {self._name}")
        if self._domain:
            filters.append(f"domain: {self._domain}")
        if self._username:
            filters.append(f"user: {self._username}")
        if self._online_only:
            filters.append("online only")
        logger.info(
            f"Enumerating ConfigMgr devices{' (' + ', '.join(filters) + ')' if filters else ''}"
        )

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            where = "WHERE 1=1"
            if self._name:
                where += f" AND sys.Name0 LIKE '%{self._name}%'"
            if self._domain:
                where += f" AND sys.Resource_Domain_OR_Workgr0 LIKE '%{self._domain}%'"
            if self._username:
                where += f" AND sys.User_Name0 LIKE '%{self._username}%'"
            if self._ip:
                where += f" AND bgb.IPAddress LIKE '%{self._ip}%'"
            if self._online_only:
                where += " AND bgb.OnlineStatus = 1"
            if self._client_only:
                where += " AND sys.Client0 = 1"

            if self._collection:
                where += f""" AND sys.ResourceID IN (
                    SELECT ResourceID FROM [{db}].dbo.v_FullCollectionMembership fcm
                    JOIN [{db}].dbo.v_Collection c ON fcm.CollectionID = c.CollectionID
                    WHERE c.Name LIKE '%{self._collection}%'
                )"""

            if self._count_only:
                count_query = f"""
SELECT COUNT(*) AS DeviceCount
FROM [{db}].dbo.v_R_System sys
LEFT JOIN [{db}].dbo.BGB_ResStatus bgb ON sys.ResourceID = bgb.ResourceID
{where};"""
                try:
                    results = database_context.query_service.execute(count_query)
                    if results:
                        logger.success(
                            f"Device count: {results[0].get('DeviceCount', 0)}"
                        )
                except Exception as ex:
                    logger.error(f"Count failed: {ex}")
                continue

            top = self._build_top_clause(self._limit)

            query = f"""
SELECT {top}
    sys.ResourceID,
    sys.Name0 AS DeviceName,
    sys.Resource_Domain_OR_Workgr0 AS Domain,
    sys.User_Name0 AS LastUser,
    bgb.IPAddress,
    sys.Operating_System_Name_and0 AS OperatingSystem,
    sys.Client0 AS HasClient,
    sys.Client_Version0 AS ClientVersion,
    sys.AD_Site_Name0 AS ADSite,
    bgb.OnlineStatus,
    bgb.LastOnlineTime
FROM [{db}].dbo.v_R_System sys
LEFT JOIN [{db}].dbo.BGB_ResStatus bgb ON sys.ResourceID = bgb.ResourceID
{where}
ORDER BY bgb.LastOnlineTime DESC;"""

            try:
                results = database_context.query_service.execute(query)
                if results:
                    logger.success(f"Found {len(results)} device(s)")
                    print(OutputFormatter.convert_list_of_dicts(results))
                else:
                    logger.warning("No devices found")
            except Exception as ex:
                logger.error(f"Failed to enumerate devices: {ex}")

        return results
