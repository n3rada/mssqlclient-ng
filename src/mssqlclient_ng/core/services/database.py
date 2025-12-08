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

        if not self._handle_impersonation():
            raise Exception("Failed to handle impersonation.")

    def _handle_impersonation(self):
        impersonate_target = self._server.impersonation_user

        if impersonate_target:
            if self._user_service.can_impersonate(impersonate_target):
                if self._user_service.impersonate_user(impersonate_target):
                    logger.info(f"Successfully impersonated user: {impersonate_target}")
                    return True
                else:
                    logger.error(f"Failed to impersonate user: {impersonate_target}")
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
