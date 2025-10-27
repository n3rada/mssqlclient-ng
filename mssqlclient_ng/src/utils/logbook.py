import sys

# Third party library imports
from loguru import logger


def _format_message(record):
    """Custom formatter with compact symbols and colors."""
    level_name = record["level"].name

    # Map levels to symbols and colors
    symbols = {
        "TRACE": ("<dim>[*]</dim>", "dim"),
        "DEBUG": ("<dim>[*]</dim>", "dim"),
        "INFO": ("<white>[i]</white>", "white"),
        "SUCCESS": ("<green>[+]</green>", "green"),
        "WARNING": ("<yellow>[!]</yellow>", "yellow"),
        "ERROR": ("<red>[-]</red>", "red"),
        "CRITICAL": ("<red><bold>[X]</bold></red>", "red"),
    }

    symbol, color = symbols.get(level_name, ("[?]", "white"))

    # Professional format: full UTC timestamp + symbol + message
    return (
        "<dim>{time:YYYY-MM-DD HH:mm:ss.SSS!UTC} (UTC)</dim> "
        f"{symbol} "
        f"<{color}>{{message}}</{color}>\n"
        "{exception}"
    )


def setup_logging(level: str = "INFO"):
    """
    Setup logging with compact, visually intuitive output.

    Args:
        level: Log level (TRACE, DEBUG, INFO, SUCCESS, WARNING, ERROR, CRITICAL)
    """
    level = level.upper()

    # Validate log level
    valid_levels = ["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]
    if level not in valid_levels:
        print(f"Invalid log level: {level}. Using INFO.")
        level = "INFO"

    # Remove all Loguru handlers to avoid duplicates
    logger.remove()

    # Add custom formatted handler
    # enqueue=False for synchronous output to maintain ordering when using print()
    logger.add(
        sys.stderr,
        enqueue=False,
        backtrace=True,
        diagnose=True,
        level=level,
        format=_format_message,
        colorize=True,
    )
