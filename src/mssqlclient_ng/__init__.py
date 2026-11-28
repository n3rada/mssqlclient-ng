# mssqlclient_ng/__init__.py

"""Impacket-based Python client for Microsoft SQL Server."""

# Built-in imports
import tomllib
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

# Third-party imports
from loguru import logger

# Local imports
from .client import MssqlClient, MssqlConfig
from .core.models.server import Server
from .core.services.authentication import AuthenticationService
from .core.services.database import DatabaseContext
from .exceptions import ConnectionError, MssqlError, QueryError

try:
    __version__ = version("mssqlclient-ng")
except PackageNotFoundError:
    try:
        pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"
        with open(pyproject_path, "rb") as f:
            pyproject_data = tomllib.load(f)
            __version__ = pyproject_data["project"]["version"] + "-dev"
    except (FileNotFoundError, KeyError):
        __version__ = "unknown"

logger.disable("mssqlclient_ng")

__all__ = [
    "AuthenticationService",
    "ConnectionError",
    "DatabaseContext",
    "MssqlClient",
    "MssqlConfig",
    "MssqlError",
    "QueryError",
    "Server",
    "__version__",
]
