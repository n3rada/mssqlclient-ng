# mssqlclient_ng/core/actions/remote/external_sources.py

# Built-in imports
from typing import Any

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter

@ActionFactory.register(
    "ext-sources",
    "Enumerate External Data Sources (Azure SQL Database, Synapse, PolyBase).",
)
class ExternalSources(BaseAction):

    def execute(
        self, database_context: DatabaseContext
    ) -> list[dict[str, Any]] | None:
        logger.info("Retrieving external data sources")

        query = "SELECT * FROM sys.external_data_sources ORDER BY name;"

        try:
            raw = database_context.query_service.execute_table(query)
        except Exception as ex:
            logger.error(f"Failed to query sys.external_data_sources: {ex}")
            return None

        if not raw:
            logger.warning("No external data sources found in the current database.")
            return None

        # Normalise to a predictable set of display columns
        display = []
        for row in raw:
            cred_id = row.get("credential_id")
            cred_str = f"ID: {cred_id}" if cred_id and str(cred_id) != "0" else ""
            display.append(
                {
                    "Name": row.get("name", ""),
                    "type": row.get("type_desc", ""),
                    "Location": row.get("location", ""),
                    "Database Name": row.get("database_name", ""),
                    "Credential": cred_str,
                    "Connection Options": row.get("connection_options", ""),
                    "Pushdown": row.get("pushdown", ""),
                }
            )

        print(OutputFormatter.convert_list_of_dicts(display))
        logger.success(f"Retrieved {len(display)} external data source(s)")
        return display
