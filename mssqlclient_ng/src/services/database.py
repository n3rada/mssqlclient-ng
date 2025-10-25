


from loguru import logger

# Local library imports
from mssqlclient_ng.src.services.authentication import AuthenticationService
from mssqlclient_ng.src.services.query import QueryService
from mssqlclient_ng.src.services.user import UserService


class DatabaseContext:
    def __init__(self, auth_service: AuthenticationService):
        self._authentication_service = auth_service
        self._server = auth_service.server
        self._query_service = QueryService(auth_service.connection)
        self._user_service = UserService(self._query_service)

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
    def authentication_service(self) -> AuthenticationService:
        return self._authentication_service

    @property
    def server(self):
        return self._server