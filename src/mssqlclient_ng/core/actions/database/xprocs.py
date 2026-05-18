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

# Descriptions for common extended procedures (xp_*)
XP_DESCRIPTIONS = {
    # Command Execution & System
    "xp_cmdshell": "Execute OS commands (requires command shell enabled)",
    "xp_servicecontrol": "Start/stop Windows services",
    "xp_terminate_process": "Terminate a Windows process by PID",
    # File System Operations
    "xp_dirtree": "List directory tree structure (depth, files)",
    "xp_subdirs": "List subdirectories only",
    "xp_fileexist": "Check if a file/directory exists",
    "xp_fixeddrives": "List drives with free space (MB)",
    "xp_availablemedia": "List available backup media devices",
    "xp_get_tape_devices": "List tape backup devices",
    "xp_create_subdir": "Create a subdirectory",
    "xp_delete_file": "Delete backup/log files",
    "xp_copy_file": "Copy a file",
    "xp_getfiledetails": "Get file size, dates, attributes",
    # Registry Operations
    "xp_regread": "Read registry value",
    "xp_regwrite": "Write registry value",
    "xp_regdeletekey": "Delete registry key",
    "xp_regdeletevalue": "Delete registry value",
    "xp_regenumkeys": "Enumerate registry subkeys",
    "xp_regenumvalues": "Enumerate registry values",
    "xp_regaddmultistring": "Add to REG_MULTI_SZ value",
    "xp_regremovemultistring": "Remove from REG_MULTI_SZ value",
    # Instance-specific Registry
    "xp_instance_regread": "Read instance registry value",
    "xp_instance_regwrite": "Write instance registry value",
    "xp_instance_regdeletekey": "Delete instance registry key",
    "xp_instance_regdeletevalue": "Delete instance registry value",
    "xp_instance_regenumkeys": "Enumerate instance registry subkeys",
    "xp_instance_regenumvalues": "Enumerate instance registry values",
    "xp_instance_regaddmultistring": "Add to instance REG_MULTI_SZ",
    "xp_instance_regremovemultistring": "Remove from instance REG_MULTI_SZ",
    # Network & SMB
    "xp_getnetname": "Get server network name",
    "xp_ntsec_enumdomains": "Enumerate trusted domains",
    "xp_enumdsn": "Enumerate ODBC data sources",
    "xp_enumgroups": "Enumerate Windows local groups",
    "xp_logininfo": "Get Windows login info from AD",
    "xp_grantlogin": "Grant Windows login access",
    "xp_revokelogin": "Revoke Windows login access",
    # SQL Server Information
    "xp_msver": "Get SQL Server version info",
    "xp_loginconfig": "Get login/auth configuration",
    "xp_readerrorlog": "Read SQL Server error log",
    "xp_enumerrorlogs": "List available error logs",
    "xp_logevent": "Write to Windows Event Log",
    # SQL Agent
    "xp_sqlagent_enum_jobs": "List SQL Agent jobs",
    "xp_sqlagent_is_starting": "Check if SQL Agent starting",
    "xp_sqlagent_monitor": "Monitor SQL Agent status",
    "xp_sqlagent_notify": "Send SQL Agent notification",
    "xp_sqlagent_param": "Get SQL Agent parameters",
    "xp_sqlmaint": "Run maintenance operations",
    # Database Mail
    "xp_sysmail_activate": "Activate Database Mail",
    "xp_sysmail_attachment_load": "Load email attachment",
    "xp_sysmail_format_query": "Format query results for email",
    "xp_sendmail": "Send email (legacy, use sp_send_dbmail)",
    "xp_smtp_sendmail": "Send email via SMTP",
    # OLE DB Providers
    "xp_enum_oledb_providers": "List installed OLE DB providers",
    "xp_prop_oledb_provider": "Get OLE DB provider properties",
    # String/Utility
    "xp_sprintf": "Format string (C-style sprintf)",
    "xp_sscanf": "Parse string (C-style sscanf)",
    "xp_qv": "Internal query processor",
    # Replication
    "xp_replposteor": "Replication end-of-record",
    "xp_repl_convert_lsn": "Convert replication LSN",
    # Misc
    "xp_msx_enlist": "Enlist in multiserver admin",
    "xp_passAgentInfo": "Pass info to SQL Agent",
}

# Descriptions for OLE Automation procedures (sp_OA*)
OLE_DESCRIPTIONS = {
    "sp_OACreate": "Create COM/OLE object instance",
    "sp_OAMethod": "Call method on COM object",
    "sp_OAGetProperty": "Get property from COM object",
    "sp_OASetProperty": "Set property on COM object",
    "sp_OAGetErrorInfo": "Get last OLE error info",
    "sp_OADestroy": "Destroy COM object instance",
    "sp_OAStop": "Stop OLE Automation environment",
}

# Descriptions for other useful system procedures
SYSTEM_PROC_DESCRIPTIONS = {
    "sp_execute_external_script": "Execute R/Python/Java scripts (requires external scripts enabled)",
    "sp_addextendedproc": "Register an extended stored procedure DLL",
    "sp_dropextendedproc": "Unregister an extended stored procedure",
    "sp_addlinkedserver": "Create a linked server",
    "sp_addlinkedsrvlogin": "Map login to linked server",
    "sp_dropserver": "Drop a linked server",
    "sp_linkedservers": "List linked servers",
    "sp_testlinkedserver": "Test linked server connectivity",
    "sp_catalogs": "List catalogs on linked server",
    "sp_tables_ex": "List tables on linked server",
    "sp_columns_ex": "List columns on linked server table",
    "sp_primarykeys": "List primary keys on linked server",
    "sp_foreignkeys": "List foreign keys on linked server",
    "sp_add_job": "Create SQL Agent job",
    "sp_add_jobstep": "Add step to SQL Agent job",
    "sp_add_jobschedule": "Add schedule to SQL Agent job",
    "sp_start_job": "Start SQL Agent job",
    "sp_stop_job": "Stop SQL Agent job",
    "sp_delete_job": "Delete SQL Agent job",
    "sp_help_job": "Get SQL Agent job info",
    "sp_add_credential": "Create a credential",
    "sp_add_proxy": "Create SQL Agent proxy",
    "sp_grant_proxy_to_subsystem": "Grant proxy to subsystem",
}


@ActionFactory.register(
    "xprocs",
    "Enumerate extended stored procedures available on SQL Server",
    aliases=["extendedprocs", "sysprocs"],
)
class ExtendedProcs(BaseAction):
    """
    Enumerate extended stored procedures available on the SQL Server instance.

    Extended procedures (xp_*) are powerful native procedures that can interact
    with the operating system, registry, and perform administrative tasks.
    This action lists all available extended procedures and checks execution permissions.
    """

    def execute(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Enumerate extended stored procedures, OLE Automation procedures,
        and system procedures on the SQL Server instance.

        Args:
            database_context: The database context

        Returns:
            List of extended procedure dictionaries with execution permissions
        """
        is_sysadmin = database_context.user_service.is_admin()
        sysadmin_flag = 1 if is_sysadmin else 0

        # ── SECTION 1: Extended Stored Procedures (xp_*) ──
        logger.info("Enumerating extended stored procedures (xp_*)")

        xp_query = f"""
            SELECT
                o.name AS [Procedure],
                CASE
                    WHEN {sysadmin_flag} = 1 THEN 'Yes (sysadmin)'
                    WHEN HAS_PERMS_BY_NAME('master.dbo.' + o.name, 'OBJECT', 'EXECUTE') = 1 THEN 'Yes'
                    ELSE 'No'
                END AS [Execute],
                o.create_date AS [Created],
                o.modify_date AS [Modified]
            FROM master.sys.all_objects o
            WHERE o.type = 'X'
                AND o.name LIKE 'xp[_]%'
            ORDER BY o.name;
        """

        try:
            xp_rows = database_context.query_service.execute_table(xp_query)

            if xp_rows:
                enriched = self._enrich_procs(xp_rows, XP_DESCRIPTIONS)
                executable = sum(
                    1 for p in enriched if str(p["Execute"]).startswith("Yes")
                )
                print(OutputFormatter.convert_list_of_dicts(enriched))
                logger.success(
                    f"Found {len(enriched)} extended procedures ({executable} executable)"
                )
            else:
                logger.warning("No extended stored procedures found or access denied")
                enriched = []

            # ── SECTION 2: OLE Automation Procedures (sp_OA*) ──
            print()
            logger.info("Enumerating OLE Automation procedures (sp_OA*)")

            ole_status = (
                database_context.configuration_service.get_configuration_status(
                    "Ole Automation Procedures"
                )
            )
            if ole_status == 1:
                logger.success("Ole Automation Procedures: Enabled")
            else:
                logger.warning(
                    "Ole Automation Procedures: Disabled (sp_OA* won't work)"
                )

            ole_query = f"""
                SELECT
                    o.name AS [Procedure],
                    CASE
                        WHEN {sysadmin_flag} = 1 THEN 'Yes (sysadmin)'
                        WHEN HAS_PERMS_BY_NAME('master.dbo.' + o.name, 'OBJECT', 'EXECUTE') = 1 THEN 'Yes'
                        ELSE 'No'
                    END AS [Execute],
                    o.create_date AS [Created],
                    o.modify_date AS [Modified]
                FROM master.sys.all_objects o
                WHERE o.type = 'X'
                    AND o.name LIKE 'sp[_]OA%'
                ORDER BY o.name;
            """

            ole_rows = database_context.query_service.execute_table(ole_query)
            if ole_rows:
                ole_enriched = self._enrich_procs(ole_rows, OLE_DESCRIPTIONS)
                print(OutputFormatter.convert_list_of_dicts(ole_enriched))
                logger.success(f"Found {len(ole_enriched)} OLE Automation procedures")

            # ── SECTION 3: Other Useful System Procedures ──
            print()
            logger.info("Checking other useful system procedures")

            ext_scripts = (
                database_context.configuration_service.get_configuration_status(
                    "external scripts enabled"
                )
            )
            clr_enabled = (
                database_context.configuration_service.get_configuration_status(
                    "clr enabled"
                )
            )
            adhoc_queries = (
                database_context.configuration_service.get_configuration_status(
                    "Ad Hoc Distributed Queries"
                )
            )

            logger.info(
                f"External Scripts (R/Python): {'Enabled' if ext_scripts == 1 else 'Disabled'}"
            )
            logger.info(
                f"CLR Integration: {'Enabled' if clr_enabled == 1 else 'Disabled'}"
            )
            logger.info(
                f"Ad Hoc Distributed Queries (OPENROWSET/OPENDATASOURCE): {'Enabled' if adhoc_queries == 1 else 'Disabled'}"
            )

            system_query = f"""
                SELECT
                    SCHEMA_NAME(o.schema_id) + '.' + o.name AS [Procedure],
                    CASE
                        WHEN {sysadmin_flag} = 1 THEN 'Yes (sysadmin)'
                        WHEN HAS_PERMS_BY_NAME(
                            QUOTENAME(DB_NAME()) + '.' + QUOTENAME(SCHEMA_NAME(o.schema_id)) + '.' + QUOTENAME(o.name),
                            'OBJECT', 'EXECUTE') = 1 THEN 'Yes'
                        ELSE 'No'
                    END AS [Execute],
                    o.type_desc AS [Type]
                FROM sys.all_objects o
                WHERE o.name IN (
                    'sp_execute_external_script',
                    'sp_addextendedproc', 'sp_dropextendedproc',
                    'sp_addlinkedserver', 'sp_addlinkedsrvlogin', 'sp_dropserver',
                    'sp_linkedservers', 'sp_testlinkedserver',
                    'sp_add_job', 'sp_add_jobstep', 'sp_start_job', 'sp_delete_job',
                    'sp_add_credential', 'sp_add_proxy'
                )
                ORDER BY o.name;
            """

            sys_rows = database_context.query_service.execute_table(system_query)
            if sys_rows:
                sys_enriched = []
                for proc in sys_rows:
                    proc_name = str(proc.get("Procedure", ""))
                    simple_name = (
                        proc_name.split(".")[-1] if "." in proc_name else proc_name
                    )
                    sys_enriched.append(
                        {
                            "Procedure": proc_name,
                            "Execute": str(proc.get("Execute", "")),
                            "Description": SYSTEM_PROC_DESCRIPTIONS.get(
                                simple_name, ""
                            ),
                            "Type": str(proc.get("Type", "")),
                        }
                    )
                sys_enriched.sort(
                    key=lambda x: (x["Execute"].startswith("No"), x["Procedure"])
                )
                print(OutputFormatter.convert_list_of_dicts(sys_enriched))
                logger.success(f"Found {len(sys_enriched)} system procedures")

            return enriched

        except Exception as e:
            logger.error(f"Failed to enumerate procedures: {e}")
            return None

    @staticmethod
    def _enrich_procs(
        rows: List[Dict[str, Any]], descriptions: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """Add descriptions and sort procedures (executable first, then by name)."""
        enriched = []
        for proc in rows:
            proc_name = str(proc.get("Procedure", ""))
            enriched.append(
                {
                    "Procedure": proc_name,
                    "Execute": str(proc.get("Execute", "")),
                    "Description": descriptions.get(proc_name, ""),
                    "Created": proc.get("Created", ""),
                    "Modified": proc.get("Modified", ""),
                }
            )
        enriched.sort(key=lambda x: (x["Execute"].startswith("No"), x["Procedure"]))
        return enriched
