# mssqlclient_ng/core/actions/remote/external_tables.py

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
    "ext-tables",
    "List external tables (PolyBase / Elastic Query / Synapse)",
)
class ExternalTables(BaseAction):
    """
    Retrieve external tables configured on the SQL Server instance.

    External tables provide virtual access to data stored outside the database:
      - Azure SQL Database: Elastic Query tables accessing remote databases
      - Azure Synapse: Tables backed by data lake files (Parquet, CSV)
      - SQL Server with PolyBase: Tables accessing Hadoop / Azure Blob Storage

    The 'Data Source' and 'Location' columns reveal the backing external source.
    With SELECT permission you can query data from those external systems.
    """

    def validate_arguments(self, additional_arguments: str = "", argument_list=None) -> None:
        pass

    def execute(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, Any]]]:
        logger.info("Retrieving external tables")

        tables_query = "SELECT * FROM sys.external_tables ORDER BY name;"

        try:
            raw_tables = database_context.query_service.execute_table(tables_query)
        except Exception as ex:
            logger.error(f"Failed to query sys.external_tables: {ex}")
            return None

        if not raw_tables:
            logger.warning("No external tables found in the current database.")
            return None

        # Build lookup dicts for data sources and file formats
        ds_dict: Dict[int, Dict[str, str]] = {}
        try:
            ds_rows = database_context.query_service.execute_table(
                "SELECT data_source_id, name, location FROM sys.external_data_sources;"
            )
            if ds_rows:
                for r in ds_rows:
                    ds_dict[int(r["data_source_id"])] = {
                        "name": r.get("name", ""),
                        "location": r.get("location", ""),
                    }
        except Exception:
            pass

        ff_dict: Dict[int, str] = {}
        try:
            ff_rows = database_context.query_service.execute_table(
                "SELECT file_format_id, name FROM sys.external_file_formats;"
            )
            if ff_rows:
                for r in ff_rows:
                    ff_dict[int(r["file_format_id"])] = r.get("name", "")
        except Exception:
            pass

        display = []
        for row in raw_tables:
            ds_id = row.get("data_source_id")
            ds_info = ds_dict.get(int(ds_id), {}) if ds_id is not None else {}

            ff_id = row.get("file_format_id")
            ff_name = ff_dict.get(int(ff_id), "") if ff_id is not None else ""

            display.append(
                {
                    "Table Name": row.get("name", ""),
                    "Schema": row.get("schema_id", ""),
                    "Data Source": ds_info.get("name", ""),
                    "Location": ds_info.get("location", ""),
                    "File Format": ff_name,
                    "Distribution": row.get("distribution_policy_desc", ""),
                    "Created": row.get("create_date", ""),
                }
            )

        print(OutputFormatter.convert_list_of_dicts(display))
        logger.success(f"Retrieved {len(display)} external table(s)")
        return display

    def get_arguments(self) -> list:
        return []
