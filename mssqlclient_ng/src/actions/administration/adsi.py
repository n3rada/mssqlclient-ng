"""
ADSI action for managing ADSI (Active Directory Service Interfaces) linked servers.
Supports listing, creating, and deleting ADSI linked servers.
"""

from typing import Optional, List
from loguru import logger

from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.services.adsi import AdsiService
from mssqlclient_ng.src.utils.common import generate_random_string
from mssqlclient_ng.src.utils.formatters import OutputFormatter


@ActionFactory.register(
    "adsi", "Manage ADSI (Active Directory Service Interfaces) linked servers"
)
class AdsiManager(BaseAction):
    """
    Manages ADSI linked servers.
    Supports listing, creating, and deleting ADSI linked servers.
    """

    def __init__(self):
        super().__init__()
        self._operation: str = "list"
        self._server_name: Optional[str] = None
        self._data_source: str = "localhost"

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate arguments for the ADSI manager action.

        Args:
            additional_arguments: Operation and parameters
                Format: <operation> [server_name] [data_source]
                Operations: list, create, delete/del/remove/rm

        Raises:
            ValueError: If arguments are invalid
        """
        if not additional_arguments or not additional_arguments.strip():
            self._operation = "list"
            return

        parts = self.split_arguments(additional_arguments)
        command = parts[0].lower()

        if command == "list":
            self._operation = "list"

        elif command == "create":
            self._operation = "create"

            # If server name not provided, generate a random one
            if len(parts) > 1 and parts[1].strip():
                self._server_name = parts[1]
            else:
                self._server_name = f"ADSI-{generate_random_string(6)}"
                logger.info(
                    f"No server name provided. Generated random name: {self._server_name}"
                )

            # Optional data source parameter
            if len(parts) > 2 and parts[2].strip():
                self._data_source = parts[2]

        elif command in ["delete", "del", "remove", "rm"]:
            self._operation = "delete"

            if len(parts) < 2 or not parts[1].strip():
                raise ValueError(
                    "Server name is required for delete operation. Example: /a:adsi delete SQL-02"
                )

            self._server_name = parts[1]

        else:
            raise ValueError(
                f"Invalid operation '{command}'. Use 'list', 'create', or 'delete'"
            )

    def execute(self, database_context: DatabaseContext) -> bool:
        """
        Execute the ADSI manager action.

        Args:
            database_context: The database context containing services

        Returns:
            True if operation succeeded; otherwise False
        """
        if self._operation == "list":
            return self._list_adsi_servers(database_context)
        elif self._operation == "create":
            return self._create_adsi_server(database_context)
        elif self._operation == "delete":
            return self._delete_adsi_server(database_context)
        else:
            logger.error("Unknown operation")
            return False

    def _list_adsi_servers(self, database_context: DatabaseContext) -> bool:
        """
        List all ADSI linked servers.

        Args:
            database_context: The database context

        Returns:
            True if servers found; otherwise False
        """
        logger.info("Enumerating ADSI linked servers")

        adsi_service = AdsiService(database_context)
        adsi_servers = adsi_service.list_adsi_servers()

        if not adsi_servers:
            logger.warning("No ADSI linked servers found")
            return False

        plural = "s" if len(adsi_servers) > 1 else ""
        logger.success(f"Found {len(adsi_servers)} ADSI linked server{plural}")

        print(OutputFormatter.convert_list(adsi_servers, "ADSI Servers"))

        return True

    def _create_adsi_server(self, database_context: DatabaseContext) -> bool:
        """
        Create a new ADSI linked server.

        Args:
            database_context: The database context

        Returns:
            True if server created successfully; otherwise False
        """
        logger.info(f"Creating ADSI linked server '{self._server_name}'")

        adsi_service = AdsiService(database_context)

        # Check if server already exists
        if adsi_service.adsi_server_exists(self._server_name):
            logger.error(f"ADSI linked server '{self._server_name}' already exists")
            return False

        success = adsi_service.create_adsi_linked_server(
            self._server_name, self._data_source
        )

        if success:
            logger.success(
                f"ADSI linked server '{self._server_name}' created successfully"
            )
            logger.info(f"Server name: {self._server_name}")
            logger.info(f"Data source: {self._data_source}")

        return success

    def _delete_adsi_server(self, database_context: DatabaseContext) -> bool:
        """
        Delete an existing ADSI linked server.

        Args:
            database_context: The database context

        Returns:
            True if server deleted successfully; otherwise False
        """
        logger.info(f"Deleting ADSI linked server '{self._server_name}'")

        adsi_service = AdsiService(database_context)

        # Check if server exists and is ADSI
        if not adsi_service.adsi_server_exists(self._server_name):
            logger.error(f"ADSI linked server '{self._server_name}' not found")
            return False

        try:
            adsi_service.drop_linked_server(self._server_name)
            logger.success(
                f"ADSI linked server '{self._server_name}' deleted successfully"
            )
            return True
        except Exception as e:
            logger.error(
                f"Failed to delete ADSI linked server '{self._server_name}': {e}"
            )
            return False

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return [
            "Operation: list, create, or delete (default: list)",
            "Server name for create/delete operations (optional for create - generates random name if omitted)",
            "Data source for the ADSI linked server (default: localhost)",
        ]
