# mssqlclient_ng/core/actions/remote/external_credentials.py

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
    "ext-creds",
    "List database-scoped credentials used by external data sources",
)
class ExternalCredentials(BaseAction):
    """
    Retrieve database-scoped credentials from sys.database_scoped_credentials.

    Database-scoped credentials store authentication information for:
      - External Data Sources (Elastic Query, PolyBase)
      - Azure Blob Storage access
      - Cross-database authentication in Azure SQL Database

    The 'In Use' column shows whether the credential is referenced by at least
    one external data source.  Credential identities may reveal architecture
    details or high-value service accounts.
    """

    def validate_arguments(self, additional_arguments: str = "", argument_list=None) -> None:
        pass

    def execute(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, Any]]]:
        logger.info("Retrieving database-scoped credentials")

        cred_query = (
            "SELECT * FROM sys.database_scoped_credentials ORDER BY create_date DESC;"
        )

        try:
            raw_creds = database_context.query_service.execute_table(cred_query)
        except Exception as ex:
            logger.error(f"Failed to query sys.database_scoped_credentials: {ex}")
            return None

        if not raw_creds:
            logger.warning(
                "No database-scoped credentials found in the current database."
            )
            return None

        # Collect credential IDs that are referenced by an external data source
        used_cred_ids: set = set()
        try:
            eds_rows = database_context.query_service.execute_table(
                "SELECT credential_id FROM sys.external_data_sources "
                "WHERE credential_id IS NOT NULL;"
            )
            if eds_rows:
                for r in eds_rows:
                    cid = r.get("credential_id")
                    if cid is not None:
                        used_cred_ids.add(int(cid))
        except Exception:
            pass  # sys.external_data_sources may not exist on older versions

        display = []
        for row in raw_creds:
            cred_id = row.get("credential_id")
            in_use = "Yes" if cred_id and int(cred_id) in used_cred_ids else "No"
            display.append(
                {
                    "ID": cred_id,
                    "Credential Name": row.get("name", ""),
                    "Identity": row.get("credential_identity", ""),
                    "Created": row.get("create_date", ""),
                    "Modified": row.get("modify_date", ""),
                    "In Use": in_use,
                }
            )

        print(OutputFormatter.convert_list_of_dicts(display))
        logger.success(f"Retrieved {len(display)} database-scoped credential(s)")
        return display

    def get_arguments(self) -> list:
        return []
