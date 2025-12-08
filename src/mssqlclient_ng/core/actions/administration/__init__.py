"""
Administration actions for SQL Server management.
"""

# Import actions to trigger registration with ActionFactory
from ..administration.config import Config
from ..administration.sessions import Sessions
from ..administration.createuser import CreateUser
from ..administration.monitor import Monitor
from ..administration.kill import Kill
from ..administration.trustworthy import Trustworthy

__all__ = [
    "Config",
    "Sessions",
    "CreateUser",
    "Monitor",
    "Kill",
    "Trustworthy",
]
