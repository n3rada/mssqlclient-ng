# mssqlclient_ng/core/actions/cloud/azure/guard.py

# Third-party imports
from loguru import logger

# Local imports
from ....services.database import DatabaseContext


def requires_azure_sql(database_context: DatabaseContext) -> bool:
    """Log an error and return False unless the execution server is Azure SQL Database."""
    qs = database_context.query_service
    cached = qs.execution_server in qs._is_azure_sql_cache
    if qs.is_azure_sql:
        return True
    if cached:
        logger.error(
            "This action requires Azure SQL Database "
            "(cached — run !info to re-check)."
        )
    else:
        logger.error(
            "This action requires Azure SQL Database. "
            "Run !info to fingerprint the target."
        )
    return False
