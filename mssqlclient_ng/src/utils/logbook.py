import sys

# Third party library imports
from loguru import logger

# Define the log format
LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS!UTC} (UTC)</green> | "
    "<level>{level: <8}</level> | "
    "<level>{message}</level>"
)


def setup_logging(level: str = "INFO"):
    level = level.upper()

    if not logger.level(level, None):
        logger.error(f"Invalid log level: {level}")
        logger.warning("Using default log level: INFO")
        level = "INFO"

    # Remove all Loguru handlers to avoid duplicates
    logger.remove()

    # enqueue=False for synchronous output to maintain ordering when using print()
    logger.add(
        sys.stderr,
        enqueue=False,
        backtrace=True,
        level=level,
        format=LOG_FORMAT,
        colorize=True,
    )
