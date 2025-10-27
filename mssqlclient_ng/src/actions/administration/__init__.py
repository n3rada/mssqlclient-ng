"""
Administration actions for SQL Server management.
"""

# Import actions to trigger registration with ActionFactory
from mssqlclient_ng.src.actions.administration.configure import Configure
from mssqlclient_ng.src.actions.administration.sessions import Sessions
from mssqlclient_ng.src.actions.administration.createuser import CreateUser
from mssqlclient_ng.src.actions.administration.monitor import Monitor
from mssqlclient_ng.src.actions.administration.kill import Kill

__all__ = [
    "Configure",
    "Sessions",
    "CreateUser",
    "Monitor",
    "Kill",
]
