# mssqlclient_ng/core/actions/cloud/rds/rds_detect.py

# Built-in imports
from typing import Any

# Third-party imports
from loguru import logger

# Local imports
from ...base import BaseAction
from ...factory import ActionFactory
from ....services.database import DatabaseContext
from ....utils.formatters import OutputFormatter


@ActionFactory.register(
    "rds-detect",
    "Detect Amazon RDS for SQL Server: enumerate rds_* procedures, check permissions, "
    "and show Multi-AZ failover status.",
    aliases=["rds"],
)
class RdsDetect(BaseAction):
    """
    Fingerprint whether the target is Amazon RDS for SQL Server.

    Checks for the rdsadmin database, enumerates rds_* stored procedures in
    msdb, tests EXECUTE permissions on each, and queries failover history.
    """

    def execute(
        self, database_context: DatabaseContext
    ) -> list[list[dict[str, Any]]] | None:
        qs = database_context.query_service

        if not qs.is_rds:
            logger.warning(
                "This does not appear to be Amazon RDS (rdsadmin database not found)"
            )
            return None

        logger.success("Amazon RDS for SQL Server detected")

        tables: list[list[dict[str, Any]]] = []

        procs = self._enumerate_procedures(qs)
        if procs:
            tables.append(procs)

        failover = self._failover_time(qs)
        if failover:
            tables.append(failover)

        return tables or None

    @staticmethod
    def _enumerate_procedures(qs) -> list[dict[str, Any]]:
        query = """
            SELECT
                p.name AS [Procedure],
                CASE
                    WHEN HAS_PERMS_BY_NAME('msdb.dbo.' + p.name, 'OBJECT', 'EXECUTE') = 1
                    THEN 'Yes' ELSE 'No'
                END AS [Can Execute]
            FROM msdb.sys.procedures p
            WHERE p.name LIKE 'rds_%'
            UNION ALL
            SELECT
                f.name AS [Procedure],
                CASE
                    WHEN HAS_PERMS_BY_NAME('msdb.dbo.' + f.name, 'OBJECT', 'EXECUTE') = 1
                    THEN 'Yes' ELSE 'No'
                END AS [Can Execute]
            FROM msdb.sys.objects f
            WHERE f.name LIKE 'rds_fn_%' AND f.type IN ('FN','IF','TF')
            ORDER BY [Procedure];
        """
        try:
            rows = qs.execute_table(query)
        except Exception as ex:
            logger.warning(f"Could not enumerate RDS procedures: {ex}")
            return []

        if not rows:
            logger.warning("No rds_* procedures found in msdb")
            return []

        executable = sum(1 for r in rows if r.get("Can Execute") == "Yes")
        logger.info(f"Found {len(rows)} RDS procedure(s), {executable} executable")
        print(OutputFormatter.convert_list_of_dicts(rows))
        return rows

    @staticmethod
    def _failover_time(qs) -> list[dict[str, Any]]:
        try:
            rows = qs.execute_table("EXEC msdb.dbo.rds_failover_time;")
        except Exception:
            logger.debug("rds_failover_time not available (single-AZ or no permission)")
            return []

        if not rows:
            return []

        logger.info("Multi-AZ failover history:")
        print(OutputFormatter.convert_list_of_dicts(rows))
        return rows
