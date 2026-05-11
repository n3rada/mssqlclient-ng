# mssqlclient_ng/core/actions/administration/audit.py

# Built-in imports
from typing import Optional, List, Dict, Any

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register("audit", "Enumerate SQL Server audit configuration")
class Audit(BaseAction):
    """
    Enumerate SQL Server audit objects and their event specifications.

    Queries sys.server_audits, sys.server_audit_specifications, and
    sys.server_audit_specification_details to reveal what actions are
    being logged, where logs are sent, and what failure behaviour is configured.

    Useful before any noisy operation:
      - Is auditing active? Which audit objects are enabled?
      - What event groups are captured (failed logins, schema changes, etc.)?
      - Where do logs go (file, Windows Event Log, Application Log)?
      - Is ON_FAILURE = SHUTDOWN set? (service dies if the audit log fills up)

    Requires VIEW SERVER STATE (held by the public role by default).
    """

    def validate_arguments(self, additional_arguments: str = "", argument_list=None) -> None:
        pass

    def execute(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, Any]]]:
        logger.info("Enumerating SQL Server audit configuration")

        audits_query = """
SELECT
    a.name,
    a.type_desc,
    a.is_state_enabled,
    a.on_failure_desc,
    a.queue_delay
FROM sys.server_audits a
ORDER BY a.name;"""

        audits = database_context.query_service.execute_table(audits_query)

        if not audits:
            logger.warning("Auditing is not configured")
            return None

        # Reformat for display
        display = []
        for row in audits:
            queue_delay = row.get("queue_delay")
            queue_str = (
                "Synchronous" if str(queue_delay) == "0" else f"{queue_delay} ms"
            )
            display.append(
                {
                    "Audit Name": row.get("name", ""),
                    "Destination": row.get("type_desc", ""),
                    "Enabled": "Yes" if row.get("is_state_enabled") else "No",
                    "On Failure": row.get("on_failure_desc", ""),
                    "Queue Delay": queue_str,
                }
            )

        logger.info(f"Found {len(audits)} audit object(s)")
        print(OutputFormatter.convert_list_of_dicts(display))

        # Audit specifications and event groups
        specs_query = """
SELECT
    a.name AS audit,
    s.name AS spec,
    s.is_state_enabled,
    d.audit_action_name
FROM sys.server_audit_specifications s
JOIN sys.server_audits a ON a.audit_guid = s.audit_guid
JOIN sys.server_audit_specification_details d
    ON d.server_specification_id = s.server_specification_id
ORDER BY a.name, s.name, d.audit_action_name;"""

        specs = database_context.query_service.execute_table(specs_query)

        if not specs:
            logger.warning("No audit specifications configured")
            logger.warning("Audit objects exist but capture nothing")
            return display

        specs_display = []
        for row in specs:
            specs_display.append(
                {
                    "Audit": row.get("audit", ""),
                    "Specification": row.get("spec", ""),
                    "Enabled": "Yes" if row.get("is_state_enabled") else "No",
                    "Event Group": row.get("audit_action_name", ""),
                }
            )

        logger.info(f"Found {len(specs)} audited event group(s)")
        print(OutputFormatter.convert_list_of_dicts(specs_display))

        return specs_display

    def get_arguments(self) -> list:
        return []
