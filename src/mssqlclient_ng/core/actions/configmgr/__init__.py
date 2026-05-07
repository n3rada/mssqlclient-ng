# mssqlclient_ng/core/actions/configmgr/__init__.py

"""ConfigMgr (SCCM) actions for database-level reconnaissance and exploitation."""

from .cm_info import CMInfo
from .cm_servers import CMServers
from .cm_collections import CMCollections
from .cm_collection import CMCollection
from .cm_devices import CMDevices
from .cm_device import CMDevice
from .cm_health import CMHealth
from .cm_deployments import CMDeployments
from .cm_deployment import CMDeployment
from .cm_deployment_types import CMDeploymentTypes
from .cm_deployment_type import CMDeploymentType
from .cm_applications import CMApplications
from .cm_packages import CMPackages
from .cm_package import CMPackage
from .cm_programs import CMPrograms
from .cm_distribution_points import CMDistributionPoints
from .cm_task_sequences import CMTaskSequences
from .cm_task_sequence import CMTaskSequence
from .cm_accounts import CMAccounts
from .cm_aad_apps import CMAadApps
from .cm_scripts import CMScripts
from .cm_script import CMScript
from .cm_script_add import CMScriptAdd
from .cm_script_delete import CMScriptDelete
from .cm_script_run import CMScriptRun
from .cm_script_status import CMScriptStatus
from .cm_rbac_add import CMRbacAdd
from .cm_log_trace import CMLogTrace

__all__ = [
    "CMInfo",
    "CMServers",
    "CMCollections",
    "CMCollection",
    "CMDevices",
    "CMDevice",
    "CMHealth",
    "CMDeployments",
    "CMDeployment",
    "CMDeploymentTypes",
    "CMDeploymentType",
    "CMApplications",
    "CMPackages",
    "CMPackage",
    "CMPrograms",
    "CMDistributionPoints",
    "CMTaskSequences",
    "CMTaskSequence",
    "CMAccounts",
    "CMAadApps",
    "CMScripts",
    "CMScript",
    "CMScriptAdd",
    "CMScriptDelete",
    "CMScriptRun",
    "CMScriptStatus",
    "CMRbacAdd",
    "CMLogTrace",
]
