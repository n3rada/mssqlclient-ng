# mssqlclient_ng/core/utils/logbook.py

"""Logbook module for logging capabilities using Loguru.

Log level guide (mirrors MSSQLand's LogLevel enum):

  TRACE   — Developer-level internal mechanics: loop detection, cache hits, RPC
             prefix matching, retry counters. Invisible to operators by default.
             Passes through logbook.silence() so diagnostic tracing remains
             visible even during silenced sub-operations (same as MSSQLand's
             Logger.TemporarilySilent which still allows Trace through).

  DEBUG   — User-facing detail the operator might need to diagnose their own
             usage: skipped servers, negative-cache hits, failed impersonation
             attempts. Suppressed by logbook.silence().

  INFO    — Normal operational progress: connection milestones, chain
             discovery, link counts. Always shown unless silenced.

  SUCCESS — Positive completion events: chain found, escalation detected.

  WARNING — Unexpected but recoverable state: no linked servers, query
             failures that fall back gracefully.

  ERROR   — Hard failures that abort the current operation.
"""

# Built-in imports
import os
import sys
import logging
import threading
from contextlib import contextmanager
from pathlib import Path

# Third party library imports
from loguru import logger

def _format_message(record):
    """Custom formatter with compact symbols and colors."""
    level_name = record["level"].name

    # Modern color palette (hex colors for better terminal support)
    trace_brown = "#8b7355"
    debug_blue = "#6c9bd1"
    info_white = "#ecf0f1"
    success_green = "#52c88a"
    warning_orange = "#f39c12"
    error_red = "#e74c3c"
    critical_magenta = "#c71585"
    time_gray = "#a5a5a5"

    # Map levels to symbols and colors
    symbols = {
        "TRACE": (f"<fg {trace_brown}>[*]</fg {trace_brown}>", trace_brown),
        "DEBUG": (f"<fg {debug_blue}>[•]</fg {debug_blue}>", debug_blue),
        "INFO": (f"<fg {info_white}>[i]</fg {info_white}>", info_white),
        "SUCCESS": (f"<fg {success_green}>[+]</fg {success_green}>", success_green),
        "WARNING": (f"<fg {warning_orange}>[!]</fg {warning_orange}>", warning_orange),
        "ERROR": (f"<fg {error_red}>[x]</fg {error_red}>", error_red),
        "CRITICAL": (
            f"<fg {critical_magenta}><bold>[!!]</bold></fg {critical_magenta}>",
            critical_magenta,
        ),
    }

    symbol, color = symbols.get(level_name, ("[?]", "white"))

    # Professional format: full UTC timestamp + symbol + message
    # Note: We must return a string template, not format it yet
    return (
        f"<fg {time_gray}>{{time:YYYY-MM-DD HH:mm:ss.SSS!UTC}} (UTC)</fg {time_gray}> "
        f"{symbol} "
        f"<fg {color}>{{message}}</fg {color}>"
        "\n{exception}"
    )

# Tracks active handler IDs so set_level can replace them without noise
_stderr_handler_id: int | None = None
_file_handler_id: int | None = None
_active_stream = sys.stderr
_active_log_file: Path | None = None

# Thread-local silence flag: when set, _silence_filter drops all log records
_silence = threading.local()


def _silence_filter(record) -> bool:
    return not getattr(_silence, "active", False)


@contextmanager
def silence():
    """Context manager: suppress all loguru output for the current thread."""
    _silence.active = True
    try:
        yield
    finally:
        _silence.active = False


def _xdg_state_dir(app_name: str = "mssqlclient-ng") -> Path:
    """Get platform-appropriate log directory following XDG standards."""

    if os.name == "nt":
        base = Path(os.getenv("LOCALAPPDATA", str(Path.home() / "AppData" / "Local")))
        return (base / app_name / "logs").resolve()

    # POSIX: follow XDG
    base = os.getenv("XDG_STATE_HOME")
    if base:
        return Path(base).expanduser().resolve() / app_name / "logs"

    return Path.home() / ".local" / "state" / app_name / "logs"

class InterceptHandler(logging.Handler):
    """Intercept standard logging and redirect to Loguru."""

    def emit(self, record: logging.LogRecord):
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where the logged message originated
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )

def setup_impacket_logging(level: str = "INFO"):
    """
    Configure Impacket's logging to use Loguru.

    Args:
        level: Log level to use for Impacket logs
    """
    # Map Loguru levels to standard logging levels
    level_mapping = {
        "TRACE": logging.DEBUG,  # Trace is more detailed than debug
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "SUCCESS": logging.INFO,  # Success treated as info
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    std_level = level_mapping.get(level.upper(), logging.INFO)

    # Intercept standard logging (used by Impacket)
    logging.basicConfig(handlers=[InterceptHandler()], level=std_level, force=True)

    # Specifically configure Impacket's logger
    for logger_name in ["impacket", "impacket.examples"]:
        impacket_logger = logging.getLogger(logger_name)
        impacket_logger.handlers = [InterceptHandler()]
        impacket_logger.setLevel(std_level)
        impacket_logger.propagate = False

def setup_logging(level: str = "INFO", stream: str = "err", enable_file: bool = True):
    """
    Setup logging with compact, visually intuitive output.

    Args:
        level: Log level (TRACE, DEBUG, INFO, SUCCESS, WARNING, ERROR, CRITICAL)
        stream: Output stream ('err' for stderr, 'out' for stdout)
        enable_file: Whether to enable file logging (default: True)
    """
    global _stderr_handler_id, _file_handler_id, _active_stream, _active_log_file

    level = level.upper()

    # Validate log level
    valid_levels = ["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]
    if level not in valid_levels:
        level = "INFO"

    # Determine output stream
    if stream == "out":
        output_stream = sys.stdout
        stream_name = "stdout"
    else:
        output_stream = sys.stderr
        stream_name = "stderr"

    _active_stream = output_stream

    # Remove all Loguru handlers to avoid duplicates
    logger.remove()

    # Add custom formatted handler
    # enqueue=False for synchronous output to maintain ordering when using print()
    _stderr_handler_id = logger.add(
        output_stream,
        enqueue=False,
        backtrace=True,
        diagnose=True,
        level=level,
        format=_format_message,
        colorize=True,
        filter=_silence_filter,
    )

    # --- File handler (rotating, UTC timestamps)
    if enable_file:
        log_dir = _xdg_state_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "mssqlclient-ng.log"
        _active_log_file = log_file

        # File format without colors
        file_format = (
            "{time:YYYY-MM-DD HH:mm:ss.SSS!UTC} (UTC) "
            "[{level:7}] {message}\n"
            "{exception}"
        )

        _file_handler_id = logger.add(
            log_file,
            format=file_format,
            level=level,
            rotation="10 MB",
            retention="14 days",
            compression="zip",
            encoding="utf-8",
            enqueue=True,  # Thread-safe
            filter=_silence_filter,
        )

        logger.trace(f"Logger initialized at level {level} on {stream_name}")
        logger.trace(f"Log file: {log_file} (rotation 10 MB, retention 14 days)")
    else:
        _file_handler_id = None
        logger.trace(f"Logger initialized at level {level} on {stream_name} (file logging disabled)")

    # Setup Impacket logging interception with same level
    setup_impacket_logging(level=level)
    logger.trace(
        f"Impacket logging intercepted and redirected to Loguru at level {level}"
    )


def get_level() -> str:
    """Return the current active log level name from the stderr handler."""
    if _stderr_handler_id is not None and _stderr_handler_id in logger._core.handlers:
        levelno = logger._core.handlers[_stderr_handler_id].levelno
        for name in ("TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"):
            if logger.level(name).no == levelno:
                return name
    return "INFO"


def set_level(level: str) -> None:
    """Silently switch the active log level without re-initializing handlers."""
    global _stderr_handler_id, _file_handler_id

    level = level.upper()
    valid_levels = ["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]
    if level not in valid_levels:
        return

    # Remove and re-add the stderr handler at the new level
    if _stderr_handler_id is not None:
        logger.remove(_stderr_handler_id)
    _stderr_handler_id = logger.add(
        _active_stream,
        enqueue=False,
        backtrace=True,
        diagnose=True,
        level=level,
        format=_format_message,
        colorize=True,
        filter=_silence_filter,
    )

    # Remove and re-add the file handler at the new level
    if _file_handler_id is not None and _active_log_file is not None:
        logger.remove(_file_handler_id)
        file_format = (
            "{time:YYYY-MM-DD HH:mm:ss.SSS!UTC} (UTC) "
            "[{level:7}] {message}\n"
            "{exception}"
        )
        _file_handler_id = logger.add(
            _active_log_file,
            format=file_format,
            level=level,
            rotation="10 MB",
            retention="14 days",
            compression="zip",
            encoding="utf-8",
            enqueue=True,
            filter=_silence_filter,
        )

    # Update impacket's standard logging level too
    setup_impacket_logging(level=level)
