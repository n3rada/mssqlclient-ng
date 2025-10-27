"""
Active Directory domain actions for SQL Server database management.
"""

# Import actions to trigger registration with ActionFactory
from mssqlclient_ng.src.actions.domain.rid_cycle import RidCycle
from mssqlclient_ng.src.actions.domain.domsid import DomainSid

__all__ = [
    "RidCycle",
    "DomainSid",
]
