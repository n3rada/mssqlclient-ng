# mssqlclient_ng/core/actions/configmgr/cm_device.py

"""Display comprehensive info about a specific ConfigMgr device."""

from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter


@ActionFactory.register("cm-device", "Display detailed info about a specific ConfigMgr device")
class CMDevice(CMBaseAction):
    """
    Display comprehensive information about a specific ConfigMgr-managed device.
    Shows device details, collection memberships, deployments, and targeted content.
    """


    def __init__(self):
        super().__init__()
        self._device_name: str = ""

    def validate_arguments(self, additional_arguments: str = "", argument_list=None) -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._device_name = named.get("name", named.get("n", "")) or self.get_positional_argument(positional, 0, "")
        if not self._device_name:
            raise ValueError("Device name is required. Usage: cm-device <device_name>")

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        logger.info(f"Retrieving device information for: {self._device_name}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)

            query = f"""
SELECT
    sys.ResourceID,
    sys.Name0 AS DeviceName,
    sys.Resource_Domain_OR_Workgr0 AS Domain,
    sys.User_Name0 AS LastUser,
    bgb.IPAddress,
    sys.Operating_System_Name_and0 AS OperatingSystem,
    os.Version0 AS OSVersion,
    sys.Client0 AS HasClient,
    sys.Client_Version0 AS ClientVersion,
    sys.AD_Site_Name0 AS ADSite,
    sys.Decommissioned0 AS Decommissioned,
    cs.Manufacturer0 AS Manufacturer,
    cs.Model0 AS Model,
    bgb.OnlineStatus,
    bgb.LastOnlineTime,
    bgb.LastOfflineTime,
    sys.Creation_Date0 AS CreationDate
FROM [{db}].dbo.v_R_System sys
LEFT JOIN [{db}].dbo.v_GS_OPERATING_SYSTEM os ON sys.ResourceID = os.ResourceID
LEFT JOIN [{db}].dbo.v_GS_COMPUTER_SYSTEM cs ON sys.ResourceID = cs.ResourceID
LEFT JOIN [{db}].dbo.BGB_ResStatus bgb ON sys.ResourceID = bgb.ResourceID
WHERE sys.Name0 = '{self._device_name}';"""

            try:
                results = database_context.query_service.execute(query)
                if not results:
                    continue

                device = results[0]
                resource_id = device.get("ResourceID")
                logger.success(f"Device: {device.get('DeviceName')} (ResourceID: {resource_id})")
                print(OutputFormatter.convert_list_of_dicts(results))

                # Collection memberships
                coll_query = f"""
SELECT c.CollectionID, c.Name AS CollectionName,
    CASE c.CollectionType WHEN 1 THEN 'User' WHEN 2 THEN 'Device' ELSE 'Other' END AS CollectionType
FROM [{db}].dbo.v_FullCollectionMembership fcm
JOIN [{db}].dbo.v_Collection c ON fcm.CollectionID = c.CollectionID
WHERE fcm.ResourceID = {resource_id}
ORDER BY c.Name;"""
                try:
                    collections = database_context.query_service.execute(coll_query)
                    if collections:
                        logger.success(f"Collection Memberships ({len(collections)})")
                        print(OutputFormatter.convert_list_of_dicts(collections))
                except Exception as ex:
                    logger.debug(f"Could not query collections: {ex}")

                return results

            except Exception as ex:
                logger.error(f"Failed to query device: {ex}")

        logger.warning(f"Device '{self._device_name}' not found")
        return None
