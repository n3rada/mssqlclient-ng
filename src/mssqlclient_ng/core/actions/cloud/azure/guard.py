# mssqlclient_ng/core/actions/cloud/azure/guard.py

# Third-party imports
from loguru import logger

# Local imports
from ....services.database import DatabaseContext


def requires_azure_sql(database_context: DatabaseContext) -> bool:
    """Log an error and return False unless the execution server is Azure SQL Database."""
    if database_context.query_service.is_azure_sql:
        return True
    logger.error(
        "This action requires Azure SQL Database. "
        "Run !info to fingerprint the target."
    )
    return False
