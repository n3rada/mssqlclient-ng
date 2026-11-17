# mssqlclient_ng/core/actions/remote/ai_models.py

# Built-in imports
from typing import Any

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter

SQL_SERVER_2025_MAJOR = 17


def requires_sql2025(database_context: DatabaseContext) -> bool:
    """Log an error and return False unless the execution server is SQL Server 2025+."""
    major = database_context.query_service.execution_major_version
    if major >= SQL_SERVER_2025_MAJOR:
        return True
    logger.error(
        "This action requires SQL Server 2025 (internal version 17+). "
        f"Detected version: {major}"
    )
    return False


@ActionFactory.register(
    "ai-models",
    "Enumerate registered external AI models (sys.external_models) and AI-related "
    "configuration options introduced in SQL Server 2025.",
    aliases=["ext-models"],
)
class AiModels(BaseAction):
    """
    Enumerate SQL Server 2025 AI configuration and registered external models.

    Shows whether 'external rest endpoint enabled' and 'external AI runtimes
    enabled' are on, then lists sys.external_models (name, type, API format,
    location and local runtime path).
    """

    def execute(
        self, database_context: DatabaseContext
    ) -> list[list[dict[str, Any]]] | None:
        if not requires_sql2025(database_context):
            return None

        logger.info("Enumerating SQL Server 2025 AI configuration and external models")

        tables = [
            table
            for table in (
                self._ai_configuration(database_context),
                self._external_models(database_context),
            )
            if table
        ]
        return tables or None

    @staticmethod
    def _ai_configuration(database_context: DatabaseContext) -> list[dict[str, Any]]:
        query = (
            "SELECT name, value_in_use FROM sys.configurations "
            "WHERE name IN ('external rest endpoint enabled', 'external AI runtimes enabled') "
            "ORDER BY name"
        )
        try:
            rows = database_context.query_service.execute_table(query)
        except Exception as ex:
            logger.warning(f"Could not query AI configuration: {ex}")
            return []

        if not rows:
            logger.warning("No AI-related configuration entries found")
            return []

        display = []
        for row in rows:
            value = int(row.get("value_in_use") or 0)
            display.append(
                {
                    "Option": row.get("name", ""),
                    "Status": "Enabled" if value == 1 else "Disabled",
                    "Value": value,
                }
            )

        logger.info("AI feature configuration:")
        print(OutputFormatter.convert_list_of_dicts(display))
        return display

    @staticmethod
    def _external_models(database_context: DatabaseContext) -> list[dict[str, Any]]:
        try:
            rows = database_context.query_service.execute_table(
                "SELECT * FROM sys.external_models ORDER BY name"
            )
        except Exception as ex:
            logger.warning(f"Could not enumerate external models: {ex}")
            return []

        if not rows:
            logger.warning(
                "No external AI models registered (sys.external_models is empty or inaccessible)"
            )
            return []

        logger.success(f"Found {len(rows)} registered external model(s)")

        display = [
            {
                "Name": row.get("name", ""),
                "Type": row.get("model_type_desc") or row.get("model_type", ""),
                "API Format": row.get("api_format", ""),
                "Location": row.get("location", ""),
                "Local Runtime Path": row.get("local_runtime_path", ""),
            }
            for row in rows
        ]
        print(OutputFormatter.convert_list_of_dicts(display))
        return display
