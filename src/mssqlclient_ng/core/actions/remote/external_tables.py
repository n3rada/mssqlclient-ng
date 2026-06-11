# mssqlclient_ng/core/actions/remote/external_tables.py

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
    "ext-tables",
    "Enumerate external tables and their remote data locations.",
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

    def execute(
        self, database_context: DatabaseContext
    ) -> list[dict[str, Any]] | None:
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
        ds_dict: dict[int, dict[str, str]] = {}
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

        ff_dict: dict[int, str] = {}
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

            # Resolve schema_id to schema name
            schema_id = row.get("schema_id")
            schema_name = "dbo"
            if schema_id is not None:
                try:
                    schema_result = database_context.query_service.execute_table(
                        f"SELECT SCHEMA_NAME({int(schema_id)}) AS s;"
                    )
                    if schema_result and schema_result[0].get("s"):
                        schema_name = schema_result[0]["s"]
                except Exception:
                    pass

            # Reject type mapping
            reject_type_raw = row.get("reject_type")
            reject_type = ""
            if reject_type_raw is not None:
                try:
                    rt = int(reject_type_raw)
                    reject_type = {0: "VALUE", 1: "PERCENTAGE"}.get(rt, str(rt))
                except (ValueError, TypeError):
                    reject_type = str(reject_type_raw)

            reject_value = str(row.get("reject_value", "") or "")
            if reject_type_raw is not None and int(reject_type_raw) == 1:
                sample = row.get("reject_sample_value")
                if sample is not None:
                    reject_value += f" (sample: {sample})"

            display.append(
                {
                    "Schema": schema_name,
                    "Table Name": row.get("name", ""),
                    "Data Source": ds_info.get("name", ""),
                    "Data Source Location": ds_info.get("location", ""),
                    "File Format": ff_name,
                    "Table Location": row.get("location", ""),
                    "Reject type": reject_type,
                    "Reject Value": reject_value,
                    "Distribution": row.get(
                        "distribution_desc", row.get("distribution_policy_desc", "")
                    ),
                }
            )

        print(OutputFormatter.convert_list_of_dicts(display))
        logger.success(f"Retrieved {len(display)} external table(s)")
        return display
