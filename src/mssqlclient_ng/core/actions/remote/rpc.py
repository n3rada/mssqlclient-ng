# mssqlclient_ng/core/actions/remote/rpc.py

# Built-in imports
from enum import Enum
from typing import Optional, List, Dict, Any

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters.formatter import OutputFormatter


class RpcActionMode(Enum):
    """RPC action mode for enabling/disabling RPC Out."""

    ENABLE = "enable"
    DISABLE = "disable"


@ActionFactory.register("rpc", "Enable or disable RPC Out option on a linked server")
class RemoteProcedureCall(BaseAction):
    """
    Manages the RPC Out option for linked servers.

    Actions (multiple aliases supported):
    - Enable: add, on, 1, true, enable
    - Disable: del, off, 0, false, disable
    """

    # Mapping of all accepted aliases to their normalized action
    ACTION_ALIASES = {
        "add": RpcActionMode.ENABLE,
        "on": RpcActionMode.ENABLE,
        "1": RpcActionMode.ENABLE,
        "true": RpcActionMode.ENABLE,
        "enable": RpcActionMode.ENABLE,
        "del": RpcActionMode.DISABLE,
        "off": RpcActionMode.DISABLE,
        "0": RpcActionMode.DISABLE,
        "false": RpcActionMode.DISABLE,
        "disable": RpcActionMode.DISABLE,
    }

    def __init__(self):
        super().__init__()
        self._action: Optional[RpcActionMode] = None
        self._linked_server_name: str = ""

    def validate_arguments(self, additional_arguments: str = "") -> None:
        """
        Validates the arguments for the RPC action.

        Args:
            additional_arguments: Action mode (add/on/1/true/enable or del/off/0/false/disable) 
                                 and linked server name

        Raises:
            ValueError: If arguments are invalid.
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Remote Procedure Call (RPC) action requires two arguments: "
                "action (add/on/1/true or del/off/0/false) and linked server name."
            )

        # Parse arguments using base class method for proper quote handling
        _, positional_args = self._parse_action_arguments(
            additional_arguments=additional_arguments
        )

        if len(positional_args) != 2:
            raise ValueError(
                "RPC action requires exactly two arguments: "
                "action (add/on/1/true or del/off/0/false) and linked server name."
            )

        # Parse action mode using alias mapping
        action_str = positional_args[0].lower()
        
        if action_str not in self.ACTION_ALIASES:
            valid_actions = ", ".join(sorted(self.ACTION_ALIASES.keys()))
            raise ValueError(
                f"Invalid action: '{positional_args[0]}'. Valid actions are: {valid_actions}"
            )
        
        self._action = self.ACTION_ALIASES[action_str]
        self._linked_server_name = positional_args[1]
        
        logger.info(
            f"RPC action: {self._action.value} on linked server '{self._linked_server_name}'"
        )

    def execute(self, database_context: DatabaseContext) -> Optional[List[Dict[str, Any]]]:
        """
        Executes the RPC action on the specified linked server.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            Result of the operation (typically empty for sp_serveroption).
        """
        rpc_value = "true" if self._action == RpcActionMode.ENABLE else "false"

        logger.info(
            f"Executing sp_serveroption to {'enable' if self._action == RpcActionMode.ENABLE else 'disable'} "
            f"RPC Out on '{self._linked_server_name}'"
        )

        query = f"""
            EXEC sp_serveroption
                 @server = '{self._linked_server_name}',
                 @optname = 'rpc out',
                 @optvalue = '{rpc_value}';
        """

        try:
            result = database_context.query_service.execute_table(query)

            if result:
                print(OutputFormatter.convert_list_of_dicts(result))

            logger.success(
                f"RPC Out {'enabled' if self._action == RpcActionMode.ENABLE else 'disabled'} "
                f"successfully on '{self._linked_server_name}'"
            )

            return result

        except Exception as e:
            logger.error(
                f"Failed to execute RPC {self._action.value} on '{self._linked_server_name}': {e}"
            )
            raise

    def get_arguments(self) -> List[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List of argument descriptions.
        """
        return [
            "Action: add/on/1/true/enable to enable RPC Out, del/off/0/false/disable to disable",
            "Linked server name"
        ]
