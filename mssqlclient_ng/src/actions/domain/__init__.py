"""
Active Directory domain actions for SQL Server database management.
"""

# Import actions to trigger registration with ActionFactory
from mssqlclient_ng.src.actions.domain.rid_cycle import RidCycle
from mssqlclient_ng.src.actions.domain.addomain import DomainSid
from mssqlclient_ng.src.actions.domain.adgroups import AdGroups

__all__ = [
    "RidCycle",
    "DomainSid",
    "AdGroups",
]
