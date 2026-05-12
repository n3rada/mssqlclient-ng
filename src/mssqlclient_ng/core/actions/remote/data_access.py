# mssqlclient_ng/core/actions/remote/data_access.py

# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext

_ENABLE_ALIASES = {"add", "on", "1", "true", "enable"}
_DISABLE_ALIASES = {"del", "off", "0", "false", "disable"}


@ActionFactory.register(
    "data",
    "Enable or disable data access (OPENQUERY) on a linked server",
)
class DataAccess(BaseAction):
    """
    Toggle the 'data access' option on a linked server via sp_serveroption.

    Data access controls whether OPENQUERY and four-part naming can retrieve
    data from the linked server.

    Enable aliases:  add, on, 1, true, enable
    Disable aliases: del, off, 0, false, disable
    """

    def __init__(self):
        super().__init__()
        self._enable: bool = True
        self._linked_server_name: str = ""

    def validate_arguments(
        self, additional_arguments: str = "", argument_list=None
    ) -> None:
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Data access action requires two arguments: "
                "<enable|disable> <linked-server-name>"
            )

        _, positional = self._parse_action_arguments(additional_arguments)

        if len(positional) != 2:
            raise ValueError(
                "Data access action requires exactly two arguments: "
                "<enable|disable> <linked-server-name>"
            )

        action_str = positional[0].lower()
        if action_str in _ENABLE_ALIASES:
            self._enable = True
        elif action_str in _DISABLE_ALIASES:
            self._enable = False
        else:
            valid = ", ".join(sorted(_ENABLE_ALIASES | _DISABLE_ALIASES))
            raise ValueError(
                f"Invalid action: '{positional[0]}'. Valid values: {valid}"
            )

        self._linked_server_name = positional[1]

    def execute(self, database_context: DatabaseContext) -> Optional[bool]:
        opt_value = "true" if self._enable else "false"
        verb = "Enabling" if self._enable else "Disabling"
        logger.info(f"{verb} data access on linked server '{self._linked_server_name}'")

        query = f"""
EXEC sp_serveroption
    @server   = '{self._linked_server_name.replace("'", "''")}',
    @optname  = 'data access',
    @optvalue = '{opt_value}';"""

        try:
            database_context.query_service.execute_non_processing(query)
        except Exception as ex:
            logger.error(
                f"Failed to modify data access on '{self._linked_server_name}': {ex}"
            )
            return False

        status = "enabled" if self._enable else "disabled"
        logger.success(
            f"Data access successfully {status} on '{self._linked_server_name}'"
        )
        if self._enable:
            logger.info("OPENQUERY operations are now available for this server.")
        else:
            logger.info("OPENQUERY operations are no longer available for this server.")
        return True

    def get_arguments(self) -> list:
        return ["<enable|disable|on|off|1|0>", "<linked-server-name>"]
