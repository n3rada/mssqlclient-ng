# mssqlclient_ng/core/actions/domain/adsi_del.py

# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
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


    def __init__(self):
        super().__init__()
        self._server_name: str = ""

    def validate_arguments(self, additional_arguments: str = "", argument_list=None) -> None:
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Server name is required. Usage: adsi-del <server_name>"
            )

        _, positional = self._parse_action_arguments(additional_arguments)

        if not positional:
            raise ValueError(
                "Server name is required. Usage: adsi-del <server_name>"
            )

        self._server_name = positional[0]

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
