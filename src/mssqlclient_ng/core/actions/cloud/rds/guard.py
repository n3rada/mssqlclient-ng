# mssqlclient_ng/core/actions/cloud/rds/guard.py

# Third-party imports
from loguru import logger

# Local imports
from ....services.database import DatabaseContext


def requires_rds(database_context: DatabaseContext) -> bool:
    """Log an error and return False unless the execution server is Amazon RDS."""
    if database_context.query_service.is_rds:
        return True
    logger.error(
        "This action requires Amazon RDS for SQL Server. "
        "Run !rds-detect to fingerprint the target."
    )
    return False
