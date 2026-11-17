# mssqlclient_ng/core/actions/cloud/azure/azure_detect.py

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
    "azure-detect",
    "Detect Azure SQL Database: service tier, edition, TDE status, "
    "geo-replication links, and feature availability.",
    aliases=["azure"],
)
class AzureDetect(BaseAction):
    """
    Fingerprint whether the target is Azure SQL Database and enumerate
    Azure-specific metadata.

    Checks engine edition, service objective, Transparent Data Encryption
    status, geo-replication links, and sp_invoke_external_rest_endpoint
    availability.
    """

    def execute(
        self, database_context: DatabaseContext
    ) -> list[list[dict[str, Any]]] | None:
        qs = database_context.query_service

        if not qs.is_azure_sql:
            logger.warning(
                "This does not appear to be Azure SQL Database "
                "(@@VERSION does not contain 'Microsoft SQL Azure')"
            )
            return None

        logger.success("Azure SQL Database detected")

        tables: list[list[dict[str, Any]]] = []

        metadata = self._service_metadata(qs)
        if metadata:
            tables.append(metadata)

        tde = self._tde_status(qs)
        if tde:
            tables.append(tde)

        geo = self._geo_replication(qs)
        if geo:
            tables.append(geo)

        features = self._feature_availability(qs)
        if features:
            tables.append(features)

        return tables or None

    @staticmethod
    def _service_metadata(qs) -> list[dict[str, Any]]:
        """Retrieve Azure SQL service tier and edition metadata."""
        queries = {
            "Engine Edition": (
                "SELECT CAST(SERVERPROPERTY('EngineEdition') AS INT)"
            ),
            "Edition": (
                "SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'Edition') AS NVARCHAR(256))"
            ),
            "Service Objective": (
                "SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'ServiceObjective') AS NVARCHAR(256))"
            ),
            "Max Size (bytes)": (
                "SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes') AS BIGINT)"
            ),
            "Elastic Pool": (
                "SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'ElasticPoolName') AS NVARCHAR(256))"
            ),
            "Contained Auth": (
                "SELECT CASE "
                "WHEN DATABASEPROPERTYEX(DB_NAME(), 'Containment') = 1 THEN 'Partial' "
                "WHEN DATABASEPROPERTYEX(DB_NAME(), 'Containment') = 0 THEN 'None' "
                "ELSE CAST(DATABASEPROPERTYEX(DB_NAME(), 'Containment') AS NVARCHAR(32)) END"
            ),
        }

        edition_names = {
            5: "Azure SQL Database",
            6: "Azure Synapse Analytics",
            8: "Azure SQL Managed Instance",
            9: "Azure SQL Edge",
            11: "Azure Synapse serverless SQL pool",
        }

        rows: list[dict[str, Any]] = []
        for label, query in queries.items():
            try:
                result = qs.execute_table(query)
                value = str(next(iter(result[0].values()))) if result else "N/A"

                if label == "Engine Edition":
                    try:
                        code = int(value)
                        value = f"{code} ({edition_names.get(code, 'Unknown')})"
                    except (ValueError, TypeError):
                        pass
                elif label == "Max Size (bytes)":
                    try:
                        size = int(value)
                        if size > 0:
                            value = f"{size / (1024 ** 3):.2f} GB"
                        else:
                            value = "Unlimited or default"
                    except (ValueError, TypeError):
                        value = "Unlimited or default"

                rows.append({"Property": label, "Value": value})
            except Exception as ex:
                logger.debug(f"Could not retrieve {label}: {ex}")

        if rows:
            logger.info("Azure SQL service metadata:")
            print(OutputFormatter.convert_list_of_dicts(rows))
        return rows

    @staticmethod
    def _tde_status(qs) -> list[dict[str, Any]]:
        """Check Transparent Data Encryption status."""
        query = (
            "SELECT db.name AS [Database], "
            "CASE tde.encryption_state "
            "  WHEN 0 THEN 'No encryption key' "
            "  WHEN 1 THEN 'Unencrypted' "
            "  WHEN 2 THEN 'Encryption in progress' "
            "  WHEN 3 THEN 'Encrypted' "
            "  WHEN 4 THEN 'Key change in progress' "
            "  WHEN 5 THEN 'Decryption in progress' "
            "  WHEN 6 THEN 'Protection change in progress' "
            "  ELSE CAST(tde.encryption_state AS NVARCHAR(10)) "
            "END AS [Encryption State], "
            "tde.encryptor_type AS [Encryptor Type], "
            "tde.key_algorithm AS [Algorithm], "
            "tde.key_length AS [Key Length] "
            "FROM sys.dm_database_encryption_keys tde "
            "JOIN sys.databases db ON tde.database_id = db.database_id;"
        )
        try:
            rows = qs.execute_table(query)
        except Exception:
            logger.debug("TDE status not available (no permission or not enabled)")
            return []

        if not rows:
            return []

        logger.info("Transparent Data Encryption status:")
        print(OutputFormatter.convert_list_of_dicts(rows))
        return rows

    @staticmethod
    def _geo_replication(qs) -> list[dict[str, Any]]:
        """Enumerate geo-replication links."""
        query = (
            "SELECT link_guid, partner_server, partner_database, "
            "role_desc AS [Role], replication_state_desc AS [State], "
            "last_replication, replication_lag_sec AS [Lag (sec)] "
            "FROM sys.geo_replication_links;"
        )
        try:
            rows = qs.execute_table(query)
        except Exception:
            logger.debug("Geo-replication view not accessible")
            return []

        if not rows:
            logger.debug("No geo-replication links found")
            return []

        logger.info(f"Found {len(rows)} geo-replication link(s):")
        print(OutputFormatter.convert_list_of_dicts(rows))
        return rows

    @staticmethod
    def _feature_availability(qs) -> list[dict[str, Any]]:
        """Probe for Azure-specific features that are pentesting-relevant."""
        checks = [
            (
                "sp_invoke_external_rest_endpoint",
                "SELECT CASE WHEN OBJECT_ID('sp_invoke_external_rest_endpoint') IS NOT NULL "
                "THEN 'Available' ELSE 'Not available' END",
            ),
            (
                "Database-scoped credentials",
                "SELECT CASE WHEN EXISTS(SELECT 1 FROM sys.database_scoped_credentials) "
                "THEN 'Present' ELSE 'None found' END",
            ),
            (
                "External data sources",
                "SELECT CASE WHEN EXISTS(SELECT 1 FROM sys.external_data_sources) "
                "THEN 'Present' ELSE 'None found' END",
            ),
            (
                "Database-level firewall rules",
                "SELECT CASE WHEN EXISTS(SELECT 1 FROM sys.database_firewall_rules) "
                "THEN 'Present' ELSE 'None found' END",
            ),
        ]

        rows: list[dict[str, Any]] = []
        for feature, query in checks:
            try:
                result = qs.execute_table(query)
                value = str(next(iter(result[0].values()))) if result else "Unknown"
                rows.append({"Feature": feature, "Status": value})
            except Exception:
                rows.append({"Feature": feature, "Status": "Check failed"})

        if rows:
            logger.info("Feature availability:")
            print(OutputFormatter.convert_list_of_dicts(rows))
        return rows
