# mssqlclient_ng/core/actions/remote/rpc.py

# Built-in imports
from enum import Enum
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


class RpcActionMode(Enum):
    """RPC action mode for enabling/disabling RPC Out."""

    ADD = "add"
    DEL = "del"


@ActionFactory.register("rpc", "Enable or disable RPC Out option on a linked server")
class RemoteProcedureCall(BaseAction):
    """
    Manages the RPC Out option for linked servers.

    Actions:
    - add: Enable RPC Out on the linked server
    - del: Disable RPC Out on the linked server
    """

    def __init__(self):
        super().__init__()
        self._action: Optional[RpcActionMode] = None
        self._linked_server_name: str = ""

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the arguments for the RPC action.

        Args:
            additional_arguments: Action mode (add/del) and linked server name

        Raises:
            ValueError: If arguments are invalid.
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Remote Procedure Call (RPC) action requires two arguments: "
                "action ('add' or 'del') and linked server name."
            )

        args = additional_arguments.strip().split(None, 1)

        if len(args) != 2:
            raise ValueError(
                "RPC action requires exactly two arguments: "
                "action ('add' or 'del') and linked server name."
            )

        # Parse action mode
        action_str = args[0].lower()
        try:
            self._action = RpcActionMode(action_str)
        except ValueError:
            valid_actions = ", ".join([mode.value for mode in RpcActionMode])
            raise ValueError(
                f"Invalid action: {args[0]}. Valid actions are: {valid_actions}."
            )

        self._linked_server_name = args[1].strip()

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        """
        Executes the RPC action on the specified linked server.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            Result of the operation (typically empty for sp_serveroption).
        """
        rpc_value = "true" if self._action == RpcActionMode.ADD else "false"

        logger.info(
            f"Executing RPC {self._action.value} on linked server '{self._linked_server_name}'"
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
                f"RPC {self._action.value} action executed successfully on '{self._linked_server_name}'"
            )

            return result

        except Exception as e:
            logger.error(
                f"Failed to execute RPC {self._action.value} on '{self._linked_server_name}': {e}"
            )
            raise

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List of argument descriptions.
        """
        return ["add|del", "linked_server_name"]
