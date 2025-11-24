"""
Administration actions for SQL Server management.
"""

# Import actions to trigger registration with ActionFactory
from mssqlclient_ng.src.actions.administration.config import Config
from mssqlclient_ng.src.actions.administration.sessions import Sessions
from mssqlclient_ng.src.actions.administration.createuser import CreateUser
from mssqlclient_ng.src.actions.administration.monitor import Monitor
from mssqlclient_ng.src.actions.administration.kill import Kill
from mssqlclient_ng.src.actions.administration.adsi import AdsiManager
from mssqlclient_ng.src.actions.administration.trustworthy import Trustworthy

__all__ = [
    "Config",
    "Sessions",
    "CreateUser",
    "Monitor",
    "Kill",
    "AdsiManager",
    "Trustworthy",
]
