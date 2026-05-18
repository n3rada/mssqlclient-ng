# mssqlclient_ng/core/actions/configmgr/cm_script_add.py

"""Upload a PowerShell script to ConfigMgr."""

import hashlib
import uuid
import random
from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService


@ActionFactory.register("cm-script-add", "Upload a PowerShell script to ConfigMgr")
class CMScriptAdd(CMBaseAction):
    """
    Upload a PowerShell script to ConfigMgr's Scripts table for later execution via cm-script-run.
    Automatically sets script to approved state. Returns script GUID needed for cm-script-run.
    """

    _script_content = Arg(position=0, long_name="content", required=True, description="PowerShell script content")
    _script_name = Arg(short_name="n", long_name="name", default="", description="Script name (auto-generated if omitted)")
    _script_guid = Arg(short_name="g", long_name="guid", default="", description="Script GUID (auto-generated if omitted)")

    def __init__(self):
        super().__init__()
        self._script_content: str = ""
        self._script_name: str = ""
        self._script_guid: str = ""

    def validate_arguments(self, additional_arguments: str = "") -> None:
        named, positional = self._parse_action_arguments(additional_arguments)

        # Script content is required - can be passed as positional or --content
        self._script_content = named.get("content", "") or self.get_positional_argument(
            positional, 0, ""
        )
        if not self._script_content:
            raise ValueError(
                "Script content is required. "
                "Usage: cm-script-add <script_content> [--name <name>] [--guid <guid>]"
            )

        # Auto-generate stealth name if not provided
        self._script_name = named.get("name", named.get("n", ""))
        if not self._script_name:
            self._script_name = f"CMDeploy0{random.randint(0, 9)}"

        # Auto-generate GUID if not provided
        self._script_guid = named.get("guid", named.get("g", ""))
        if not self._script_guid:
            self._script_guid = str(uuid.uuid4()).upper()

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        logger.info(f"Adding ConfigMgr script: {self._script_name}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            try:
                # Encode script as UTF-16LE
                script_bytes = self._script_content.encode("utf-16-le")
                script_hex = script_bytes.hex().upper()

                # Calculate SHA256 hash
                script_hash = hashlib.sha256(script_bytes).hexdigest().upper()

                insert_query = f"""
INSERT INTO [{db}].dbo.Scripts
(ScriptGuid, ScriptVersion, ScriptName, Script, ScriptType, Approver, ApprovalState, Feature, Author, LastUpdateTime, ScriptHash, Comment, ScriptDescription)
VALUES
('{self._script_guid}', 1, '{self._script_name}', 0x{script_hex}, 0, 'CM', 3, 1, 'CM', '', '{script_hash}', '', '');"""

                database_context.query_service.execute_non_processing(insert_query)

                logger.success("Script added successfully")
                logger.info(f"  GUID: {self._script_guid}")
                logger.info(f"  Name: {self._script_name}")
                logger.info(f"  Hash: {script_hash}")
                logger.info(
                    "Use 'cm-script-run --resourceid <ID> --scriptguid "
                    + self._script_guid
                    + "' to execute"
                )

                return [
                    {"ScriptGuid": self._script_guid, "ScriptName": self._script_name}
                ]

            except Exception as ex:
                logger.error(f"Failed to add script: {ex}")

        return None
