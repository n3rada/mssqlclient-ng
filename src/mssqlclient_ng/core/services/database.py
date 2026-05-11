# mssqlclient_ng/core/services/database.py

from typing import TYPE_CHECKING

# Third party imports
from loguru import logger

# Local library imports
from .query import QueryService
from .user import UserService
from .configuration import ConfigurationService

if TYPE_CHECKING:
    from impacket.tds import MSSQL
    from ..models.server import Server


class DatabaseContext:
    def __init__(self, server: "Server", mssql_instance: "MSSQL"):
        self._server = server
        self._query_service = QueryService(mssql_instance)
        self._user_service = UserService(self._query_service)
        self._config_service = ConfigurationService(self._query_service, self._server)

        self._server.hostname = self._query_service.execution_server

        # Store pre-impersonation identity (matches MSSQLand: GetInfo() before impersonation)
        pre_user, pre_system = self._user_service.get_info()
        self._pre_impersonation_user = pre_user
        self._pre_impersonation_system = pre_system

        # Compute effective user and source before impersonation changes SYSTEM_USER
        # (matches MSSQLand: ComputeEffectiveUserAndSource() in constructor before impersonation)
        if self._user_service.is_domain_user:
            self._user_service.compute_effective_user_and_source()

        if not self._handle_impersonation():
            raise Exception("Failed to handle impersonation.")

    @property
    def pre_impersonation_user(self) -> str:
        """The mapped user before impersonation was applied."""
        return self._pre_impersonation_user

    @property
    def pre_impersonation_system(self) -> str:
        """The system user (login) before impersonation was applied."""
        return self._pre_impersonation_system

    def _handle_impersonation(self):
        targets = self._server.impersonation_users

        for target in targets:
            if self._user_service.can_impersonate(target):
                if self._user_service.impersonate_user(target):
                    logger.debug(f"Successfully impersonated user: {target}")
                else:
                    logger.error(f"Failed to impersonate user: {target}")
                    return False
            else:
                logger.error(f"Cannot impersonate user: {target}")
                return False
        return True

    @property
    def user_service(self) -> UserService:
        return self._user_service

    @property
    def query_service(self) -> QueryService:
        return self._query_service

    @property
    def config_service(self) -> ConfigurationService:
        return self._config_service

    @property
    def server(self):
        return self._server
