"""
Administration actions for SQL Server management.
"""

# Import actions to trigger registration with ActionFactory
from mssqlclientng.src.actions.administration.configure import Configure
from mssqlclientng.src.actions.administration.sessions import Sessions
from mssqlclientng.src.actions.administration.createuser import CreateUser
from mssqlclientng.src.actions.administration.monitor import Monitor
from mssqlclientng.src.actions.administration.kill import Kill
from mssqlclientng.src.actions.administration.adsi import AdsiManager

__all__ = [
    "Configure",
    "Sessions",
    "CreateUser",
    "Monitor",
    "Kill",
    "AdsiManager",
]
