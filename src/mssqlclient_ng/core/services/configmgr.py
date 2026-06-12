# mssqlclient_ng/core/services/configmgr.py

"""ConfigMgr (SCCM) database detection and utility service."""

import re
import xml.etree.ElementTree as ET
from typing import Any

from loguru import logger

from .query import QueryService

class CMService:
    """
    Service for ConfigMgr/SCCM database detection and common operations.
    Mirrors MSSQLand's CMService.cs functionality.
    """

    def __init__(self, query_service: QueryService):
        self._query_service = query_service
        self._has_views_cache: dict[str, bool] = {}

    def get_sccm_databases(self) -> list[str]:
        """
        Gets all ConfigMgr databases on the server.
        If the current execution database is a CM_ database, returns only that one.
        """
        # Check if we're already in a CM_ database context
        try:
            db_result = self._query_service.execute_scalar("SELECT DB_NAME()")
            if db_result and str(db_result).upper().startswith("CM_"):
                logger.debug(f"Current database '{db_result}' is a ConfigMgr database")
                return [str(db_result)]
        except Exception:
            pass

        # Query for all ConfigMgr databases
        try:
            rows = self._query_service.execute(
                "SELECT name FROM sys.databases WHERE name LIKE 'CM_%';"
            )
            return [row["name"] for row in rows if row.get("name")]
        except Exception as ex:
            logger.debug(f"Failed to enumerate ConfigMgr databases: {ex}")
            return []

    @staticmethod
    def get_site_code(database_name: str) -> str | None:
        """Extracts the site code from a ConfigMgr database name (e.g., CM_PSC -> PSC)."""
        if not database_name or not database_name.upper().startswith("CM_"):
            return None
        return database_name[3:]

    def has_sccm_views(self) -> bool:
        """Checks if the ConfigMgr database has vSMS_* views (newer versions) or uses base tables."""
        server = self._query_service.execution_server
        if server in self._has_views_cache:
            return self._has_views_cache[server]

        try:
            result = self._query_service.execute_scalar(
                "SELECT COUNT(*) FROM sys.views WHERE name LIKE 'vSMS_%'"
            )
            view_count = int(result) if result else 0
            has_views = view_count > 0
            logger.debug(
                f"Found {view_count} vSMS_* views - Using {'views' if has_views else 'base tables'}"
            )
            self._has_views_cache[server] = has_views
            return has_views
        except Exception:
            logger.debug(
                "Failed to check for vSMS_* views, falling back to base tables"
            )
            self._has_views_cache[server] = False
            return False

    @staticmethod
    def decode_offer_type(offer_type) -> str:
        if offer_type is None:
            return "Unknown"
        val = int(offer_type)
        return {0: "Required", 2: "Available"}.get(val, f"Unknown ({val})")

    @staticmethod
    def decode_feature_type(feature_type) -> str:
        if feature_type is None:
            return "Unknown"
        val = int(feature_type)
        mapping = {
            1: "Application",
            2: "Program",
            3: "Mobile Program",
            4: "Script",
            5: "Software Update",
            6: "Baseline",
            7: "Task Sequence",
            8: "Content Distribution",
            9: "Distribution Point Group",
            10: "Distribution Point Health",
            11: "Configuration Policy",
        }
        return mapping.get(val, f"Unknown ({val})")

    @staticmethod
    def decode_deployment_intent(intent) -> str:
        if intent is None:
            return "Unknown"
        val = int(intent)
        return {1: "Required", 2: "Available", 3: "Simulate"}.get(
            val, f"Unknown ({val})"
        )

    @staticmethod
    def decode_package_type(package_type) -> str:
        if package_type is None:
            return "Package"
        val = int(package_type)
        mapping = {
            0: "Package",
            3: "Driver Package",
            4: "Task Sequence",
            5: "Software Update",
            6: "Device Setting",
            7: "Virtual App",
            8: "Application",
            257: "OS Image",
            258: "Boot Image",
            259: "OS Installer",
        }
        return mapping.get(val, f"Unknown ({val})")

    @staticmethod
    def decode_remote_client_flags(flags_val) -> str:
        if flags_val is None:
            return "None"
        flags = int(flags_val)
        result = []

        # Rerun behavior
        if flags & 0x00000800:
            result.append("RERUN_ALWAYS")
        elif flags & 0x00001000:
            result.append("RERUN_NEVER")
        elif flags & 0x00002000:
            result.append("RERUN_IF_FAILED")
        elif flags & 0x00004000:
            result.append("RERUN_IF_SUCCEEDED")

        # Distribution point behavior
        if flags & 0x00000008:
            result.append("RUN_FROM_LOCAL_DISPPOINT")
        if flags & 0x00000010:
            result.append("DOWNLOAD_FROM_LOCAL_DISPPOINT")
        if flags & 0x00000020:
            result.append("DONT_RUN_NO_LOCAL_DISPPOINT")
        if flags & 0x00000040:
            result.append("DOWNLOAD_FROM_REMOTE_DISPPOINT")
        if flags & 0x00000080:
            result.append("RUN_FROM_REMOTE_DISPPOINT")
        if flags & 0x00000100:
            result.append("DOWNLOAD_ON_DEMAND_FROM_LOCAL_DP")
        if flags & 0x00000200:
            result.append("DOWNLOAD_ON_DEMAND_FROM_REMOTE_DP")

        return ", ".join(result) if result else "None"

    @staticmethod
    def decode_program_flags(flags_val) -> str:
        if flags_val is None:
            return "None"
        flags = int(flags_val)
        result = []
        flag_defs = {
            0x00000001: "AUTHORIZED_DYNAMIC_INSTALL",
            0x00000010: "DEFAULT_PROGRAM",
            0x00001000: "DISABLED",
            0x00002000: "UNATTENDED",
            0x00004000: "USERCONTEXT",
            0x00008000: "ADMINRIGHTS",
            0x00010000: "EVERYUSER",
            0x00020000: "NOUSERLOGGEDIN",
            0x00040000: "OKTOQUIT",
            0x00080000: "OKTOREBOOT",
            0x00100000: "USEUNCPATH",
            0x00400000: "RUNMINIMIZED",
            0x00800000: "RUNMAXIMIZED",
            0x01000000: "HIDEWINDOW",
            0x02000000: "OKTOLOGOFF",
            0x08000000: "ANY_PLATFORM",
            0x20000000: "SUPPORT_UNINSTALL",
        }
        for mask, name in flag_defs.items():
            if flags & mask:
                result.append(name)
        return "; ".join(result) if result else "None"

    @staticmethod
    def parse_sdm_package_digest(
        xml_content: str, detailed: bool = False
    ) -> dict[str, str]:
        """Parse SDMPackageDigest XML and extract deployment type information."""
        info: dict[str, str] = {}
        if not xml_content:
            return info

        try:
            # Define namespaces
            ns = {
                "p1": "http://schemas.microsoft.com/SystemCenterConfigurationManager/2009/AppMgmtDigest",
            }
            root = ET.fromstring(xml_content)

            # Basic fields
            tech = root.find(
                ".//{http://schemas.microsoft.com/SystemCenterConfigurationManager/2009/AppMgmtDigest}Technology"
            )
            info["Technology"] = tech.text if tech is not None and tech.text else ""

            install = root.find(
                ".//{http://schemas.microsoft.com/SystemCenterConfigurationManager/2009/AppMgmtDigest}InstallCommandLine"
            )
            info["InstallCommand"] = (
                install.text if install is not None and install.text else ""
            )

            location = root.find(
                ".//{http://schemas.microsoft.com/SystemCenterConfigurationManager/2009/AppMgmtDigest}Location"
            )
            info["ContentLocation"] = (
                location.text if location is not None and location.text else ""
            )

            exec_ctx = root.find(
                ".//{http://schemas.microsoft.com/SystemCenterConfigurationManager/2009/AppMgmtDigest}ExecutionContext"
            )
            info["ExecutionContext"] = (
                exec_ctx.text if exec_ctx is not None and exec_ctx.text else ""
            )

            # Detection type
            enhanced = root.find(
                ".//{http://schemas.microsoft.com/SystemCenterConfigurationManager/2009/AppMgmtDigest}EnhancedDetectionMethod"
            )
            if enhanced is not None:
                info["DetectionType"] = "Enhanced"
            else:
                detect = root.find(
                    ".//{http://schemas.microsoft.com/SystemCenterConfigurationManager/2009/AppMgmtDigest}DetectAction"
                )
                info["DetectionType"] = "Script" if detect is not None else ""

            if detailed:
                uninstall = root.find(
                    ".//{http://schemas.microsoft.com/SystemCenterConfigurationManager/2009/AppMgmtDigest}UninstallCommandLine"
                )
                info["UninstallCommand"] = (
                    uninstall.text if uninstall is not None and uninstall.text else ""
                )

                # Detection method summary
                detection_methods = []
                if "<File" in xml_content:
                    detection_methods.append("File-based detection")
                if "<RegistryKey" in xml_content:
                    detection_methods.append("Registry-based detection")
                if "<Script" in xml_content:
                    detection_methods.append("Script-based detection")
                if "ProductCode" in xml_content:
                    detection_methods.append("MSI Product Code detection")
                info["DetectionMethodSummary"] = (
                    ", ".join(detection_methods) if detection_methods else "Unknown"
                )

        except Exception as ex:
            logger.debug(f"Failed to parse SDMPackageDigest XML: {ex}")

        return info

    @staticmethod
    def build_top_clause(limit: int) -> str:
        """Build SQL TOP clause from limit value."""
        return f"TOP {limit}" if limit > 0 else ""
