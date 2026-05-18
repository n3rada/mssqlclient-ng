# mssqlclient_ng/core/actions/remote/external_sources.py

# Built-in imports
from typing import Optional, List, Dict, Any

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "ext-sources",
    "List external data sources (PolyBase, Azure Elastic Query, Azure Synapse)",
)
class ExternalSources(BaseAction):
    """
    Retrieve external data sources configured on the SQL Server instance.

    External data sources enable querying data stored outside the database:
      - Azure SQL Database: Elastic Query for cross-database queries
      - Azure Synapse Analytics: Query data lakes (Parquet, CSV in ADLS)
      - SQL Server with PolyBase: Access Hadoop, Azure Blob Storage, etc.

    Unlike linked servers (server-to-server connections), external data sources
    are designed for cloud storage integration and distributed architectures.
    """

    def execute(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, Any]]]:
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
                    "Type": row.get("type_desc", ""),
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
