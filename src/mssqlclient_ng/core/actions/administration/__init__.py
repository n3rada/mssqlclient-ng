"""
Administration actions for SQL Server management.
"""

# Import actions to trigger registration with ActionFactory
from ..administration.audit import Audit
from ..administration.config import Config
from ..administration.sessions import Sessions
from ..administration.createuser import CreateUser
from ..administration.kill import Kill
from ..administration.requests import Requests

__all__ = [
    "Audit",
    "Config",
    "Sessions",
    "CreateUser",
    "Kill",
    "Requests",
]
