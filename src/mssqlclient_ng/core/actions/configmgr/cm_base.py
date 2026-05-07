# mssqlclient_ng/core/actions/configmgr/cm_base.py

"""Base class for ConfigMgr actions providing common SCCM database detection logic."""

from typing import Optional, List
from loguru import logger

from ..base import BaseAction
from ...services.database import DatabaseContext
from ...services.configmgr import CMService


class CMBaseAction(BaseAction):
    """
    Base action for ConfigMgr operations.
    Provides common database detection and iteration pattern.
    """

    def _get_cm_service(self, database_context: DatabaseContext) -> CMService:
        return CMService(database_context.query_service)

    def _get_databases(self, database_context: DatabaseContext) -> List[str]:
        """Get ConfigMgr databases, logging warnings if none found."""
        cm_service = self._get_cm_service(database_context)
        databases = cm_service.get_sccm_databases()
        if not databases:
            logger.warning("No ConfigMgr databases found")
        return databases

    @staticmethod
    def _build_top_clause(limit: int) -> str:
        return f"TOP {limit}" if limit > 0 else ""
