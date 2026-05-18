# mssqlclient_ng/core/actions/agent/job_proxies.py

# Built-in imports
from typing import Optional, List, Dict, Any

# Third party imports
from loguru import logger

# Local library imports
from ..base import Arg, BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "job-proxies",
    "Enumerate SQL Server Agent proxy accounts, subsystem mappings, and login mappings",
)
class JobProxies(BaseAction):
    """
    Enumerate SQL Server Agent proxy accounts.

    Proxy accounts allow job steps to run under alternate Windows credentials —
    useful for discovering stored credentials in the MSDB database.

    Shows three tables:
      1. Proxy accounts with credential names and identities
      2. Proxy -> Subsystem mappings (which subsystems the proxy can run)
      3. Proxy -> Login mappings (which logins can use the proxy)
    """

    _limit: int = Arg(short_name="l", long_name="limit", default=25, description="Cap result count")  # type: ignore[assignment]

    def validate_arguments(self, additional_arguments: str = "") -> None:
        super().validate_arguments(additional_arguments)
        self._limit = int(self._limit)

    def execute(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, Any]]]:
        logger.info("Enumerating SQL Server Agent proxy accounts")

        top_clause = f"TOP {self._limit}" if self._limit > 0 else ""

        # Proxy accounts with credential identity
        proxy_query = f"""
SELECT {top_clause}
    p.proxy_id AS ProxyId,
    p.name AS ProxyName,
    p.enabled AS Enabled,
    c.name AS CredentialName,
    c.credential_identity AS CredentialIdentity,
    p.description AS Description
FROM msdb.dbo.sysproxies p
LEFT JOIN sys.credentials c ON p.credential_id = c.credential_id
ORDER BY p.name;"""

        proxies = database_context.query_service.execute_table(proxy_query)

        if not proxies:
            logger.info("No proxy accounts found.")
            return None

        print(OutputFormatter.convert_list_of_dicts(proxies))
        logger.success(f"Found {len(proxies)} proxy account(s)")

        # Proxy -> Subsystem mappings
        logger.info("Proxy Subsystem Mappings:")
        subsystem_query = """
SELECT
    p.name AS ProxyName,
    sub.subsystem AS Subsystem,
    sub.agent_exe AS AgentExe
FROM msdb.dbo.sysproxysubsystem ps
JOIN msdb.dbo.sysproxies p ON ps.proxy_id = p.proxy_id
JOIN msdb.dbo.syssubsystems sub ON ps.subsystem_id = sub.subsystem_id
ORDER BY p.name, sub.subsystem;"""

        subsystems = database_context.query_service.execute_table(subsystem_query)

        if not subsystems:
            logger.info("  No subsystem mappings found")
        else:
            print(OutputFormatter.convert_list_of_dicts(subsystems))
            logger.success(f"Found {len(subsystems)} subsystem mapping(s)")

        # Proxy -> Login mappings
        logger.info("Proxy Login Mappings:")
        login_query = """
SELECT
    p.name AS ProxyName,
    SUSER_SNAME(pl.sid) AS LoginName,
    CASE pl.flags
        WHEN 0 THEN 'SQL Login / Windows User'
        WHEN 1 THEN 'Server Role'
        WHEN 2 THEN 'msdb Role'
        ELSE CAST(pl.flags AS VARCHAR)
    END AS LoginType
FROM msdb.dbo.sysproxylogin pl
JOIN msdb.dbo.sysproxies p ON pl.proxy_id = p.proxy_id
ORDER BY p.name;"""

        logins = database_context.query_service.execute_table(login_query)

        if not logins:
            logger.info("  No login mappings found")
        else:
            print(OutputFormatter.convert_list_of_dicts(logins))
            logger.success(f"Found {len(logins)} login mapping(s)")

        return proxies
