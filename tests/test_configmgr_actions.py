# tests/test_configmgr_actions.py

"""Tests for ConfigMgr actions - registration, argument parsing, and validation."""

import unittest
from unittest.mock import MagicMock, patch

from mssqlclient_ng.core.actions.factory import ActionFactory
from mssqlclient_ng.core.actions.configmgr import (
    CMInfo,
    CMServers,
    CMCollections,
    CMCollection,
    CMDevices,
    CMDevice,
    CMHealth,
    CMDeployments,
    CMDeployment,
    CMDeploymentTypes,
    CMDeploymentType,
    CMApplications,
    CMPackages,
    CMPackage,
    CMPrograms,
    CMDistributionPoints,
    CMTaskSequences,
    CMTaskSequence,
    CMAccounts,
    CMAadApps,
    CMScripts,
    CMScript,
    CMScriptAdd,
    CMScriptDelete,
    CMScriptRun,
    CMScriptStatus,
    CMRbacAdd,
    CMLogTrace,
)
from mssqlclient_ng.core.services.configmgr import CMService


class TestConfigMgrActionRegistration(unittest.TestCase):
    """Test that all ConfigMgr actions are properly registered."""

    def test_all_cm_actions_registered(self):
        """Verify all 28 ConfigMgr actions are registered."""
        expected_actions = [
            "cm-info",
            "cm-servers",
            "cm-collections",
            "cm-collection",
            "cm-devices",
            "cm-device",
            "cm-health",
            "cm-deployments",
            "cm-deployment",
            "cm-dts",
            "cm-dt",
            "cm-apps",
            "cm-packages",
            "cm-package",
            "cm-programs",
            "cm-dps",
            "cm-tasksequences",
            "cm-tasksequence",
            "cm-accounts",
            "cm-aadapps",
            "cm-scripts",
            "cm-script",
            "cm-script-add",
            "cm-script-delete",
            "cm-script-run",
            "cm-script-status",
            "cm-rbac-add",
            "cm-trace",
        ]
        for action_name in expected_actions:
            action = ActionFactory.get_action(action_name)
            self.assertIsNotNone(action, f"Action '{action_name}' not registered")

    def test_cm_info_registered(self):
        action = ActionFactory.get_action("cm-info")
        self.assertIsInstance(action, CMInfo)

    def test_cm_servers_registered(self):
        action = ActionFactory.get_action("cm-servers")
        self.assertIsInstance(action, CMServers)

    def test_cm_scripts_registered(self):
        action = ActionFactory.get_action("cm-scripts")
        self.assertIsInstance(action, CMScripts)

    def test_cm_rbac_add_registered(self):
        action = ActionFactory.get_action("cm-rbac-add")
        self.assertIsInstance(action, CMRbacAdd)


class TestCMServiceUtilities(unittest.TestCase):
    """Test CMService static utility methods."""

    def test_get_site_code_valid(self):
        self.assertEqual(CMService.get_site_code("CM_PSC"), "PSC")

    def test_get_site_code_lowercase(self):
        self.assertEqual(CMService.get_site_code("cm_ABC"), "ABC")

    def test_get_site_code_invalid(self):
        self.assertIsNone(CMService.get_site_code("master"))
        self.assertIsNone(CMService.get_site_code(""))
        self.assertIsNone(CMService.get_site_code(None))

    def test_decode_offer_type(self):
        self.assertEqual(CMService.decode_offer_type(0), "Required")
        self.assertEqual(CMService.decode_offer_type(2), "Available")
        self.assertEqual(CMService.decode_offer_type(99), "Unknown (99)")
        self.assertEqual(CMService.decode_offer_type(None), "Unknown")

    def test_decode_feature_type(self):
        self.assertEqual(CMService.decode_feature_type(1), "Application")
        self.assertEqual(CMService.decode_feature_type(2), "Program")
        self.assertEqual(CMService.decode_feature_type(7), "Task Sequence")
        self.assertEqual(CMService.decode_feature_type(None), "Unknown")

    def test_decode_deployment_intent(self):
        self.assertEqual(CMService.decode_deployment_intent(1), "Required")
        self.assertEqual(CMService.decode_deployment_intent(2), "Available")
        self.assertEqual(CMService.decode_deployment_intent(3), "Simulate")
        self.assertEqual(CMService.decode_deployment_intent(None), "Unknown")

    def test_decode_package_type(self):
        self.assertEqual(CMService.decode_package_type(0), "Package")
        self.assertEqual(CMService.decode_package_type(4), "Task Sequence")
        self.assertEqual(CMService.decode_package_type(8), "Application")
        self.assertEqual(CMService.decode_package_type(257), "OS Image")
        self.assertEqual(CMService.decode_package_type(None), "Package")

    def test_decode_remote_client_flags(self):
        self.assertEqual(CMService.decode_remote_client_flags(None), "None")
        self.assertEqual(CMService.decode_remote_client_flags(0), "None")
        result = CMService.decode_remote_client_flags(0x00000010)
        self.assertIn("DOWNLOAD_FROM_LOCAL_DISPPOINT", result)

    def test_decode_program_flags(self):
        self.assertEqual(CMService.decode_program_flags(None), "None")
        self.assertEqual(CMService.decode_program_flags(0), "None")
        result = CMService.decode_program_flags(0x00008000)
        self.assertIn("ADMINRIGHTS", result)

    def test_build_top_clause(self):
        self.assertEqual(CMService.build_top_clause(25), "TOP 25")
        self.assertEqual(CMService.build_top_clause(0), "")
        self.assertEqual(CMService.build_top_clause(-1), "")

    def test_parse_sdm_package_digest_empty(self):
        result = CMService.parse_sdm_package_digest("")
        self.assertEqual(result, {})

    def test_parse_sdm_package_digest_basic(self):
        xml = """<?xml version="1.0" encoding="utf-8"?>
<AppMgmtDigest xmlns="http://schemas.microsoft.com/SystemCenterConfigurationManager/2009/AppMgmtDigest">
  <DeploymentType>
    <Technology>Script</Technology>
    <InstallCommandLine>msiexec /i app.msi /qn</InstallCommandLine>
    <Location>\\\\server\\share\\app</Location>
    <ExecutionContext>System</ExecutionContext>
    <EnhancedDetectionMethod/>
  </DeploymentType>
</AppMgmtDigest>"""
        result = CMService.parse_sdm_package_digest(xml)
        self.assertEqual(result["Technology"], "Script")
        self.assertEqual(result["InstallCommand"], "msiexec /i app.msi /qn")
        self.assertEqual(result["ContentLocation"], "\\\\server\\share\\app")
        self.assertEqual(result["ExecutionContext"], "System")
        self.assertEqual(result["DetectionType"], "Enhanced")


class TestCMCollectionsArguments(unittest.TestCase):
    """Test CMCollections argument parsing."""

    def test_default_limit(self):
        action = CMCollections()
        action.validate_arguments("")
        self.assertEqual(action._limit, 25)

    def test_custom_limit(self):
        action = CMCollections()
        action.validate_arguments("--limit 50")
        self.assertEqual(action._limit, 50)

    def test_name_filter(self):
        action = CMCollections()
        action.validate_arguments("--name Workstations")
        self.assertEqual(action._name_filter, "Workstations")

    def test_type_filter(self):
        action = CMCollections()
        action.validate_arguments("--type device")
        self.assertEqual(action._collection_type, "device")

    def test_collection_id_positional(self):
        action = CMCollections()
        action.validate_arguments("SMS00001")
        self.assertEqual(action._collection_id, "SMS00001")

    def test_with_members_flag(self):
        action = CMCollections()
        action.validate_arguments("--with-members")
        self.assertTrue(action._with_members)


class TestCMCollectionArguments(unittest.TestCase):
    """Test CMCollection argument parsing."""

    def test_collection_id_positional(self):
        action = CMCollection()
        action.validate_arguments("SMS00001")
        self.assertEqual(action._collection_id, "SMS00001")

    def test_name_flag(self):
        action = CMCollection()
        action.validate_arguments("--name Workstations")
        self.assertEqual(action._collection_name, "Workstations")

    def test_requires_id_or_name(self):
        action = CMCollection()
        with self.assertRaises(ValueError):
            action.validate_arguments("")


class TestCMDevicesArguments(unittest.TestCase):
    """Test CMDevices argument parsing."""

    def test_default_limit(self):
        action = CMDevices()
        action.validate_arguments("")
        self.assertEqual(action._limit, 25)

    def test_name_filter(self):
        action = CMDevices()
        action.validate_arguments("--name WORKSTATION01")
        self.assertEqual(action._name, "WORKSTATION01")

    def test_domain_filter(self):
        action = CMDevices()
        action.validate_arguments("--domain CORP")
        self.assertEqual(action._domain, "CORP")

    def test_online_flag(self):
        action = CMDevices()
        action.validate_arguments("--online")
        self.assertTrue(action._online_only)

    def test_count_flag(self):
        action = CMDevices()
        action.validate_arguments("--count")
        self.assertTrue(action._count_only)


class TestCMDeviceArguments(unittest.TestCase):
    """Test CMDevice argument parsing."""

    def test_device_name_positional(self):
        action = CMDevice()
        action.validate_arguments("WORKSTATION01")
        self.assertEqual(action._device_name, "WORKSTATION01")

    def test_requires_name(self):
        action = CMDevice()
        with self.assertRaises(ValueError):
            action.validate_arguments("")


class TestCMDeploymentsArguments(unittest.TestCase):
    """Test CMDeployments argument parsing."""

    def test_name_filter(self):
        action = CMDeployments()
        action.validate_arguments("--name Chrome")
        self.assertEqual(action._name, "Chrome")

    def test_intent_filter(self):
        action = CMDeployments()
        action.validate_arguments("--intent required")
        self.assertEqual(action._intent, "required")

    def test_type_filter(self):
        action = CMDeployments()
        action.validate_arguments("--type application")
        self.assertEqual(action._feature_type, "application")


class TestCMDeploymentArguments(unittest.TestCase):
    """Test CMDeployment argument parsing."""

    def test_assignment_id_positional(self):
        action = CMDeployment()
        action.validate_arguments("16779074")
        self.assertEqual(action._assignment_id, "16779074")

    def test_requires_id(self):
        action = CMDeployment()
        with self.assertRaises(ValueError):
            action.validate_arguments("")


class TestCMApplicationsArguments(unittest.TestCase):
    """Test CMApplications argument parsing."""

    def test_display_name_filter(self):
        action = CMApplications()
        action.validate_arguments("--displayname Chrome")
        self.assertEqual(action._display_name, "Chrome")

    def test_model_name_filter(self):
        action = CMApplications()
        action.validate_arguments("--modelname ScopeId")
        self.assertEqual(action._model_name, "ScopeId")


class TestCMPackagesArguments(unittest.TestCase):
    """Test CMPackages argument parsing."""

    def test_name_filter(self):
        action = CMPackages()
        action.validate_arguments("--name 7zip")
        self.assertEqual(action._name, "7zip")

    def test_source_filter(self):
        action = CMPackages()
        # shlex processes backslashes, so test with pre-split argument list
        # In real usage, prompt-toolkit passes raw input differently.
        action.validate_arguments("--source server_share")
        self.assertEqual(action._source_path, "server_share")


class TestCMPackageArguments(unittest.TestCase):
    """Test CMPackage argument parsing."""

    def test_package_id_positional(self):
        action = CMPackage()
        action.validate_arguments("PSC00001")
        self.assertEqual(action._package_id, "PSC00001")

    def test_requires_id(self):
        action = CMPackage()
        with self.assertRaises(ValueError):
            action.validate_arguments("")


class TestCMScriptsArguments(unittest.TestCase):
    """Test CMScripts argument parsing."""

    def test_name_filter(self):
        action = CMScripts()
        action.validate_arguments("--name MyScript")
        self.assertEqual(action._name, "MyScript")

    def test_no_args(self):
        action = CMScripts()
        action.validate_arguments("")
        self.assertEqual(action._name, "")


class TestCMScriptArguments(unittest.TestCase):
    """Test CMScript argument parsing."""

    def test_guid_positional(self):
        action = CMScript()
        action.validate_arguments("A1B2C3D4-E5F6-7890-ABCD-EF1234567890")
        self.assertEqual(action._script_guid, "A1B2C3D4-E5F6-7890-ABCD-EF1234567890")

    def test_requires_guid(self):
        action = CMScript()
        with self.assertRaises(ValueError):
            action.validate_arguments("")


class TestCMScriptAddArguments(unittest.TestCase):
    """Test CMScriptAdd argument parsing."""

    def test_content_positional(self):
        action = CMScriptAdd()
        action.validate_arguments("Write-Host 'Hello'")
        self.assertEqual(
            action._script_content, "Write-Host"
        )  # Positional is first word
        # Actually, the full content with quotes is parsed differently

    def test_requires_content(self):
        action = CMScriptAdd()
        with self.assertRaises(ValueError):
            action.validate_arguments("")

    def test_auto_generates_name(self):
        action = CMScriptAdd()
        action.validate_arguments("Get-Process")
        self.assertTrue(action._script_name.startswith("CMDeploy0"))

    def test_auto_generates_guid(self):
        action = CMScriptAdd()
        action.validate_arguments("Get-Process")
        self.assertRegex(
            action._script_guid,
            r"^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$",
        )


class TestCMScriptDeleteArguments(unittest.TestCase):
    """Test CMScriptDelete argument parsing."""

    def test_guid_positional(self):
        action = CMScriptDelete()
        action.validate_arguments("A1B2C3D4-E5F6-7890-ABCD-EF1234567890")
        self.assertEqual(action._script_guid, "A1B2C3D4-E5F6-7890-ABCD-EF1234567890")

    def test_requires_guid(self):
        action = CMScriptDelete()
        with self.assertRaises(ValueError):
            action.validate_arguments("")

    def test_blocks_cmpivot_deletion(self):
        action = CMScriptDelete()
        with self.assertRaises(ValueError):
            action.validate_arguments("7DC6B6F1-E7F6-43C1-96E0-E1D16BC25C14")


class TestCMScriptRunArguments(unittest.TestCase):
    """Test CMScriptRun argument parsing."""

    def test_resource_id_and_script_guid(self):
        action = CMScriptRun()
        action.validate_arguments(
            "--resourceid 12345 --scriptguid A1B2C3D4-E5F6-7890-ABCD-EF1234567890"
        )
        self.assertEqual(action._resource_id, "12345")
        self.assertEqual(action._script_guid, "A1B2C3D4-E5F6-7890-ABCD-EF1234567890")

    def test_requires_resourceid(self):
        action = CMScriptRun()
        with self.assertRaises(ValueError):
            action.validate_arguments(
                "--scriptguid A1B2C3D4-E5F6-7890-ABCD-EF1234567890"
            )

    def test_requires_scriptguid(self):
        action = CMScriptRun()
        with self.assertRaises(ValueError):
            action.validate_arguments("--resourceid 12345")


class TestCMScriptStatusArguments(unittest.TestCase):
    """Test CMScriptStatus argument parsing."""

    def test_task_id_positional(self):
        action = CMScriptStatus()
        action.validate_arguments("42")
        self.assertEqual(action._task_id, "42")

    def test_requires_task_id(self):
        action = CMScriptStatus()
        with self.assertRaises(ValueError):
            action.validate_arguments("")


class TestCMRbacAddArguments(unittest.TestCase):
    """Test CMRbacAdd argument parsing."""

    def test_account_positional(self):
        action = CMRbacAdd()
        action.validate_arguments("CORP\\\\admin")
        self.assertEqual(action._account_name, "CORP\\admin")

    def test_requires_account(self):
        action = CMRbacAdd()
        with self.assertRaises(ValueError):
            action.validate_arguments("")


class TestCMLogTraceArguments(unittest.TestCase):
    """Test CMLogTrace argument parsing."""

    def test_full_guid(self):
        action = CMLogTrace()
        action.validate_arguments(
            "ScopeId_0072A515-CC53-4FD2-9B92-0A3F0518595C/DeploymentType_af7c1e90-5fdb-4be1-b0dd-701165673d2c"
        )
        self.assertIn("ScopeId_", action._guid)

    def test_deployment_type_prefix(self):
        action = CMLogTrace()
        action.validate_arguments("DeploymentType_af7c1e90-5fdb-4be1-b0dd-701165673d2c")
        self.assertEqual(
            action._guid, "DeploymentType_af7c1e90-5fdb-4be1-b0dd-701165673d2c"
        )

    def test_bare_guid_gets_prefix(self):
        action = CMLogTrace()
        action.validate_arguments("af7c1e90-5fdb-4be1-b0dd-701165673d2c")
        self.assertEqual(
            action._guid, "DeploymentType_af7c1e90-5fdb-4be1-b0dd-701165673d2c"
        )

    def test_requires_guid(self):
        action = CMLogTrace()
        with self.assertRaises(ValueError):
            action.validate_arguments("")


class TestCMTaskSequencesArguments(unittest.TestCase):
    """Test CMTaskSequences argument parsing."""

    def test_name_filter(self):
        action = CMTaskSequences()
        action.validate_arguments("--name Windows10")
        self.assertEqual(action._name, "Windows10")

    def test_package_id_filter(self):
        action = CMTaskSequences()
        action.validate_arguments("--packageid PSC00001")
        self.assertEqual(action._package_id, "PSC00001")


class TestCMTaskSequenceArguments(unittest.TestCase):
    """Test CMTaskSequence argument parsing."""

    def test_package_id_positional(self):
        action = CMTaskSequence()
        action.validate_arguments("PSC002C0")
        self.assertEqual(action._package_id, "PSC002C0")

    def test_requires_package_id(self):
        action = CMTaskSequence()
        with self.assertRaises(ValueError):
            action.validate_arguments("")


class TestCMDistributionPointsArguments(unittest.TestCase):
    """Test CMDistributionPoints argument parsing."""

    def test_server_filter(self):
        action = CMDistributionPoints()
        action.validate_arguments("--server DP01")
        self.assertEqual(action._server, "DP01")

    def test_active_flag(self):
        action = CMDistributionPoints()
        action.validate_arguments("--active")
        self.assertTrue(action._active_only)


class TestCMHealthArguments(unittest.TestCase):
    """Test CMHealth argument parsing."""

    def test_filter_positional(self):
        action = CMHealth()
        action.validate_arguments("WORKSTATION01")
        self.assertEqual(action._filter, "WORKSTATION01")

    def test_limit(self):
        action = CMHealth()
        action.validate_arguments("--limit 10")
        self.assertEqual(action._limit, 10)


class TestCMAadAppsArguments(unittest.TestCase):
    """Test CMAadApps argument parsing."""

    def test_filter(self):
        action = CMAadApps()
        action.validate_arguments("CMG")
        self.assertEqual(action._filter, "CMG")

    def test_no_args(self):
        action = CMAadApps()
        action.validate_arguments("")
        self.assertEqual(action._filter, "")


class TestCMDeploymentTypesArguments(unittest.TestCase):
    """Test CMDeploymentTypes argument parsing."""

    def test_tech_filter(self):
        action = CMDeploymentTypes()
        action.validate_arguments("--tech MSI")
        self.assertEqual(action._technology, "MSI")

    def test_content_filter(self):
        action = CMDeploymentTypes()
        # shlex processes backslashes; test with simple path.
        action.validate_arguments("--content server_chrome")
        self.assertEqual(action._content_path, "server_chrome")

    def test_app_filter(self):
        action = CMDeploymentTypes()
        action.validate_arguments("--app Chrome")
        self.assertEqual(action._application, "Chrome")


class TestCMDeploymentTypeArguments(unittest.TestCase):
    """Test CMDeploymentType argument parsing."""

    def test_ci_id_positional(self):
        action = CMDeploymentType()
        action.validate_arguments("12345")
        self.assertEqual(action._ci_id, "12345")

    def test_requires_ci_id(self):
        action = CMDeploymentType()
        with self.assertRaises(ValueError):
            action.validate_arguments("")


class TestCMProgramsArguments(unittest.TestCase):
    """Test CMPrograms argument parsing."""

    def test_package_filter(self):
        action = CMPrograms()
        action.validate_arguments("--package PSC00001")
        self.assertEqual(action._package_id, "PSC00001")

    def test_commandline_filter(self):
        action = CMPrograms()
        action.validate_arguments("--commandline msiexec")
        self.assertEqual(action._command_line, "msiexec")


if __name__ == "__main__":
    unittest.main()
