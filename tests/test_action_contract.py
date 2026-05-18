# tests/test_action_contract.py

"""Enforce the ActionResult return contract for all registered actions.

Two rules are checked for every action via a mocked database context:

  1. Shape rule — if execute() returns non-None, the value must be
       list[dict]            (single tabular result)
       list[list[dict]]      (multiple independent tables)
     Bare dict, str, bool, tuple, object, etc. are not valid.

  2. Completeness rule — if the action called
       OutputFormatter.convert_list_of_dicts()
     during execute(), the return value must NOT be None.
     Returning None after rendering tabular output means the data
     was printed but not cached, so format changes have no effect.

Actions that do not call convert_list_of_dicts() and return None
(side-effect / mutation / unstructured-output actions) are valid
and pass both checks.
"""

import pytest
from unittest.mock import MagicMock, patch

# Import all action sub-packages at module level so ActionFactory.list_actions()
# is fully populated when pytest collects the parametrize values.
import mssqlclient_ng.core.actions.agent  # noqa: F401
import mssqlclient_ng.core.actions.administration  # noqa: F401
import mssqlclient_ng.core.actions.configmgr  # noqa: F401
import mssqlclient_ng.core.actions.database  # noqa: F401
import mssqlclient_ng.core.actions.domain  # noqa: F401
import mssqlclient_ng.core.actions.execution  # noqa: F401
import mssqlclient_ng.core.actions.filesystem  # noqa: F401
import mssqlclient_ng.core.actions.remote  # noqa: F401

from mssqlclient_ng.core.actions.factory import ActionFactory

_SAMPLE_ROWS = [{"col": "val"}]
_CONVERT_TARGET = (
    "mssqlclient_ng.core.utils.formatters.formatter.OutputFormatter.convert_list_of_dicts"
)


def _make_mock_db_context() -> MagicMock:
    """Return a MagicMock database context that satisfies the most common action needs."""
    ctx = MagicMock()

    # Every query method returns one sample row so branches that guard on empty
    # results don't short-circuit before reaching the interesting code paths.
    for method in ("execute", "execute_table", "execute_server"):
        getattr(ctx.query_service, method).return_value = list(_SAMPLE_ROWS)

    ctx.query_service.execution_server = "LAB-SQL01"
    ctx.query_service.execution_database = "master"

    ctx.user_service.system_user = "sa"
    ctx.user_service.mapped_user = "dbo"
    ctx.user_service.is_admin.return_value = False
    ctx.user_service.get_info.return_value = ("dbo", "sa")
    ctx.user_service.get_server_roles.return_value = ([], [])

    return ctx


def _is_valid_result_shape(value) -> bool:
    """
    Return True when *value* is a valid ActionResult shape:
      - None
      - list[dict]
      - list[list[dict]]
    """
    if value is None:
        return True
    if not isinstance(value, list):
        return False
    if not value:
        return True  # empty list is fine
    first = value[0]
    if isinstance(first, dict):
        return True
    if isinstance(first, list):
        return all(
            isinstance(item, dict)
            for sub in value
            if isinstance(sub, list)
            for item in sub
        )
    return False


@pytest.mark.parametrize("action_name", sorted(ActionFactory.list_actions()))
class TestActionReturnContract:
    """Parametrised suite — one test instance per registered action."""

    def _run_execute(self, action_name: str):
        """
        Instantiate the action, bind empty arguments, run execute() with a mock
        context.  Returns (result, convert_was_called).

        Raises pytest.skip.Exception when the action cannot be exercised without
        real arguments or a live database.
        """
        action = ActionFactory.get_action(action_name)
        if action is None:
            pytest.skip(f"'{action_name}': could not instantiate")

        try:
            action.validate_arguments("")
        except Exception as exc:
            pytest.skip(f"'{action_name}': validate_arguments raised {type(exc).__name__}: {exc}")

        ctx = _make_mock_db_context()

        with patch(_CONVERT_TARGET, wraps=lambda data: "") as mock_convert, \
                patch("builtins.print"):
            try:
                result = action.execute(database_context=ctx)
            except Exception as exc:
                pytest.skip(f"'{action_name}': execute raised {type(exc).__name__}: {exc}")

            return result, mock_convert.called

    def test_shape(self, action_name: str):
        """If execute() returns non-None, the value must be list[dict] or list[list[dict]]."""
        result, _ = self._run_execute(action_name)

        assert _is_valid_result_shape(result), (
            f"Action '{action_name}' ({type(ActionFactory.get_action(action_name)).__name__}) "
            f"returned an invalid ActionResult.\n"
            f"  Expected: None, list[dict], or list[list[dict]]\n"
            f"  Got: {type(result).__name__} = {result!r}"
        )

    def test_completeness(self, action_name: str):
        """If convert_list_of_dicts was called, execute() must not return None."""
        result, convert_called = self._run_execute(action_name)

        if convert_called and result is None:
            cls_name = type(ActionFactory.get_action(action_name)).__name__
            pytest.fail(
                f"Action '{action_name}' ({cls_name}) called "
                f"OutputFormatter.convert_list_of_dicts() but returned None.\n"
                f"  Print the rows AND return them so the cache can re-render "
                f"on format changes:\n"
                f"    print(OutputFormatter.convert_list_of_dicts(rows))\n"
                f"    return rows"
            )
