"""
Extended Procedures action for enumerating extended stored procedures available on SQL Server.
"""

# Built-in imports
from typing import Optional, List, Dict, Any

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


# Descriptions for common extended procedures
PROCEDURE_DESCRIPTIONS = {
    "xp_cmdshell": "Executes command shell commands",
    "xp_dirtree": "Displays directory tree structure",
    "xp_fileexist": "Checks if a file exists",
    "xp_fixeddrives": "Lists fixed drives and their free space",
    "xp_regread": "Reads registry values",
    "xp_regwrite": "Writes registry values",
    "xp_regdeletekey": "Deletes registry keys",
    "xp_regdeletevalue": "Deletes registry values",
    "xp_regenumkeys": "Enumerates registry keys",
    "xp_regenumvalues": "Enumerates registry values",
    "xp_regaddmultistring": "Adds multistring registry values",
    "xp_regremovemultistring": "Removes multistring registry values",
    "xp_instance_regread": "Reads instance-specific registry values",
    "xp_instance_regwrite": "Writes instance-specific registry values",
    "xp_instance_regdeletekey": "Deletes instance-specific registry keys",
    "xp_instance_regdeletevalue": "Deletes instance-specific registry values",
    "xp_instance_regenumkeys": "Enumerates instance-specific registry keys",
    "xp_instance_regenumvalues": "Enumerates instance-specific registry values",
    "xp_instance_regaddmultistring": "Adds instance-specific multistring registry values",
    "xp_instance_regremovemultistring": "Removes instance-specific multistring registry values",
    "xp_servicecontrol": "Starts/stops SQL Server services",
    "xp_subdirs": "Lists subdirectories",
    "xp_create_subdir": "Creates a subdirectory",
    "xp_delete_file": "Deletes a file",
    "xp_delete_files": "Deletes multiple files",
    "xp_copy_file": "Copies a file",
    "xp_copy_files": "Copies multiple files",
    "xp_getnetname": "Returns the network name of the server",
    "xp_msver": "Returns SQL Server version information",
    "xp_loginconfig": "Returns login configuration information",
    "xp_logevent": "Logs events to Windows Event Log",
    "xp_sprintf": "Formats strings (similar to C sprintf)",
    "xp_sscanf": "Parses strings (similar to C sscanf)",
    "xp_enum_oledb_providers": "Lists OLE DB providers",
    "xp_prop_oledb_provider": "Returns OLE DB provider properties",
    "xp_readerrorlog": "Reads SQL Server error log",
    "xp_enumerrorlogs": "Lists SQL Server error logs",
    "xp_enumgroups": "Lists Windows groups",
    "xp_availablemedia": "Lists available backup media",
    "xp_get_tape_devices": "Lists tape devices",
    "xp_sqlagent_enum_jobs": "Lists SQL Agent jobs",
    "xp_sqlagent_is_starting": "Checks if SQL Agent is starting",
    "xp_sqlagent_monitor": "Monitors SQL Agent",
    "xp_sqlagent_notify": "Sends notifications via SQL Agent",
    "xp_sqlagent_param": "Gets SQL Agent parameters",
    "xp_sqlmaint": "Maintenance utility",
    "xp_sysmail_activate": "Activates Database Mail",
    "xp_sysmail_attachment_load": "Loads email attachments",
    "xp_sysmail_format_query": "Formats query results for email",
    "xp_replposteor": "Replication-related procedure",
    "xp_passAgentInfo": "Passes information to SQL Agent",
    "xp_msx_enlist": "Enlists server in multiserver environment",
    "xp_qv": "Internal query processor procedure",
}


@ActionFactory.register(
    "xprocs", "Enumerate extended stored procedures available on SQL Server"
)
class ExtendedProcs(BaseAction):
    """
    Enumerate extended stored procedures available on the SQL Server instance.

    Extended procedures (xp_*) are powerful native procedures that can interact
    with the operating system, registry, and perform administrative tasks.
    This action lists all available extended procedures and checks execution permissions.
    """

    def __init__(self):
        super().__init__()

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate arguments (none required for this action).

        Args:
            additional_arguments: Not used
        """
        # No arguments needed
        pass

    def execute(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Enumerate extended stored procedures on the SQL Server instance.

        Args:
            database_context: The database context

        Returns:
            List of extended procedure dictionaries with execution permissions
        """
        logger.info("Enumerating extended stored procedures...")

        try:
            # Check if user is sysadmin
            is_sysadmin = database_context.user_service.is_admin()

            query = f"""
                SELECT
                    o.name AS [Procedure Name],
                    CASE
                        WHEN {1 if is_sysadmin else 0} = 1 THEN 'Yes (sysadmin)'
                        WHEN HAS_PERMS_BY_NAME('master.dbo.' + o.name, 'OBJECT', 'EXECUTE') = 1 THEN 'Yes'
                        ELSE 'No'
                    END AS [Execute],
                    o.create_date AS [Created Date],
                    o.modify_date AS [Modified Date]
                FROM master.sys.all_objects o
                WHERE o.type = 'X'
                    AND o.name LIKE 'xp_%'
                ORDER BY o.name;
            """

            result_rows = database_context.query_service.execute_table(query)

            if not result_rows:
                logger.warning("No extended stored procedures found or access denied")
                return None

            # Add descriptions and sort
            enriched_procs = []
            for proc in result_rows:
                proc_name = proc["Procedure Name"]

                execute_val = proc["Execute"]
                if isinstance(execute_val, bytes):
                    execute_val = execute_val.decode("utf-8")
                else:
                    execute_val = str(execute_val)

                enriched_proc = {
                    "Procedure Name": proc_name,
                    "Execute": execute_val,
                    "Description": PROCEDURE_DESCRIPTIONS.get(proc_name, ""),
                    "Created Date": proc["Created Date"],
                    "Modified Date": proc["Modified Date"],
                }
                enriched_procs.append(enriched_proc)

            # Sort: Execute DESC (Yes before No), then Procedure Name ASC
            enriched_procs.sort(
                key=lambda x: (
                    x["Execute"].startswith("No"),  # False (Yes) comes before True (No)
                    x["Procedure Name"],
                )
            )

            logger.success(f"Found {len(enriched_procs)} extended stored procedures")
            print(OutputFormatter.convert_list_of_dicts(enriched_procs))

            return enriched_procs

        except Exception as e:
            logger.error(f"Failed to enumerate extended stored procedures: {e}")
            return None

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            Empty list (no arguments required)
        """
        return []
