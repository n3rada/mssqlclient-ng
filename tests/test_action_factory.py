# tests/test_action_factory.py

"""Tests for ActionFactory — registration, aliases, lookup, and clearing."""

import pytest

from mssqlclient_ng.core.actions.factory import ActionFactory
from mssqlclient_ng.core.actions.base import BaseAction


# ── Fixtures ────────────────────────────────────────────────────────────


class DummyAction(BaseAction):
    """Minimal action for testing factory registration."""

    def validate_arguments(self, additional_arguments: str = "") -> None:
        pass

    def execute(self, database_context=None) -> None:
        return "dummy_result"


# ── Tests ───────────────────────────────────────────────────────────────


class TestActionFactoryRegistration:
    """Test manual registration and lookup."""

    def setup_method(self):
        """Snapshot and restore registry around each test."""
        self._original_registry = dict(ActionFactory._registry)
        self._original_aliases = dict(ActionFactory._aliases)

    def teardown_method(self):
        ActionFactory._registry = self._original_registry
        ActionFactory._aliases = self._original_aliases

    def test_register_and_get(self):
        ActionFactory.register("test-dummy", "A test action")(DummyAction)
        action = ActionFactory.get_action("test-dummy")
        assert action is not None
        assert isinstance(action, DummyAction)

    def test_get_nonexistent(self):
        assert ActionFactory.get_action("nonexistent-action-xyz") is None

    def test_case_insensitive(self):
        ActionFactory.register("test-UPPER", "Upper case")(DummyAction)
        action = ActionFactory.get_action("TEST-upper")
        assert action is not None

    def test_action_exists(self):
        ActionFactory.register("test-exists", "Exists check")(DummyAction)
        assert ActionFactory.action_exists("test-exists")
        assert not ActionFactory.action_exists("test-does-not-exist-xyz")

    def test_list_actions_includes_registered(self):
        ActionFactory.register("test-list", "Listed")(DummyAction)
        assert "test-list" in ActionFactory.list_actions()

    def test_get_action_description(self):
        ActionFactory.register("test-desc", "My description")(DummyAction)
        assert ActionFactory.get_action_description("test-desc") == "My description"

    def test_get_action_description_nonexistent(self):
        assert ActionFactory.get_action_description("nope-xyz") is None

    def test_get_action_type(self):
        ActionFactory.register("test-type", "Type check")(DummyAction)
        action_class = ActionFactory._registry["test-type"][0]
        assert action_class == DummyAction

    def test_get_action_type_nonexistent(self):
        assert ActionFactory._registry.get("nope-xyz") is None

    def test_clear_registry(self):
        ActionFactory.register("test-clear", "Will be cleared")(DummyAction)
        ActionFactory.clear_registry()
        assert ActionFactory.get_action("test-clear") is None
        # Restore for teardown
        ActionFactory._registry = dict(self._original_registry)


class TestActionFactoryAliases:
    """Test alias resolution."""

    def setup_method(self):
        self._original_registry = dict(ActionFactory._registry)
        self._original_aliases = dict(ActionFactory._aliases)

    def teardown_method(self):
        ActionFactory._registry = self._original_registry
        ActionFactory._aliases = self._original_aliases

    def test_alias_resolves(self):
        ActionFactory.register("test-canonical", "Canonical")(DummyAction)
        ActionFactory._aliases["tc"] = "test-canonical"
        action = ActionFactory.get_action("tc")
        assert isinstance(action, DummyAction)

    def test_alias_exists(self):
        ActionFactory._aliases["myalias"] = "test-canonical"
        assert ActionFactory.action_exists("myalias")

    def test_list_aliases(self):
        ActionFactory._aliases["a1"] = "target1"
        aliases = ActionFactory.list_aliases()
        assert "a1" in aliases
        assert aliases["a1"] == "target1"


class TestActionFactoryDecorator:
    """Test the @ActionFactory.register decorator."""

    def setup_method(self):
        self._original_registry = dict(ActionFactory._registry)
        self._original_aliases = dict(ActionFactory._aliases)

    def teardown_method(self):
        ActionFactory._registry = self._original_registry
        ActionFactory._aliases = self._original_aliases

    def test_decorator_registers(self):
        @ActionFactory.register("test-decorated", "Decorated action")
        class DecoratedAction(BaseAction):
            def validate_arguments(self, additional_arguments=""):
                pass

            def execute(self, database_context=None):
                pass

        action = ActionFactory.get_action("test-decorated")
        assert isinstance(action, DecoratedAction)

    def test_decorator_with_aliases(self):
        @ActionFactory.register(
            "test-aliased", "Aliased action", aliases=["ta", "talias"]
        )
        class AliasedAction(BaseAction):
            def validate_arguments(self, additional_arguments=""):
                pass

            def execute(self, database_context=None):
                pass

        assert ActionFactory.get_action("ta") is not None
        assert ActionFactory.get_action("talias") is not None


class TestBuiltinActionsRegistered:
    """Smoke tests ensuring core actions are loaded by import."""

    def test_whoami_registered(self):
        assert ActionFactory.action_exists("whoami")

    def test_databases_registered(self):
        assert ActionFactory.action_exists("databases")

    def test_exec_registered(self):
        assert ActionFactory.action_exists("exec")

    def test_impersonate_registered(self):
        assert ActionFactory.action_exists("impersonate")
