# mssqlclient_ng/core/actions/domain/adsi_del.py

# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction, Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.adsi import AdsiService


@ActionFactory.register(
    "adsi-del", "Delete an ADSI linked server", aliases=["adsi-delete", "adsi-drop"]
)
class AdsiDel(BaseAction):
    """
    Deletes an ADSI linked server by name.
    """

    _server_name = Arg(position=0, required=True, description="ADSI linked server name to delete")

    def execute(self, database_context: DatabaseContext) -> Optional[bool]:
        adsi_service = AdsiService(database_context)

        if not adsi_service.adsi_server_exists(self._server_name):
            logger.error(f"ADSI linked server '{self._server_name}' not found.")
            return False

        try:
            adsi_service.drop_linked_server(self._server_name)
            logger.success(
                f"ADSI linked server '{self._server_name}' deleted successfully"
            )
            return True
        except Exception as ex:
            logger.error(
                f"Failed to delete ADSI linked server '{self._server_name}': {ex}"
            )
            return False

    def get_arguments(self) -> list:
        return ["<server_name>"]
