"""Common output options in interactive and CLI execution."""

import csv
import io
import json
import shlex
import sys
from unittest.mock import MagicMock, patch

import pytest

from mssqlclient_ng import cli
from mssqlclient_ng.core.actions.factory import ActionFactory
from mssqlclient_ng.core.terminal import Terminal
from mssqlclient_ng.core.utils import command_output
from mssqlclient_ng.core.utils.formatters import OutputFormatter


@pytest.fixture(autouse=True)
def restore_format():
    previous = OutputFormatter.current_format()
    yield
    OutputFormatter.set_format(previous)


def test_query_export_preserves_sql(mock_database_context, tmp_path, capsys):
    path = tmp_path / "task history.csv"
    path.write_text("old output", encoding="utf-8")
    statement = r"EXEC msdb.dbo.rds_task_status @db_name = N'corp\archive';"
    mock_database_context.query_service.execute_table.return_value = [
        {"task_id": 42, "description": "résumé, complete"}
    ]
    terminal = Terminal(mock_database_context)
    previous_format = OutputFormatter.current_format()

    terminal._dispatch_input(f'!query --save "{path}" --format csv -- {statement}')

    saved = path.read_text(encoding="utf-8")
    assert saved == capsys.readouterr().out
    assert list(csv.DictReader(io.StringIO(saved.lstrip()), delimiter=";")) == [
        {"task_id": "42", "description": "résumé, complete"}
    ]
    assert OutputFormatter.current_format() == previous_format
    mock_database_context.query_service.execute_table.assert_called_once_with(statement)
    mock_database_context.query_service.sync_execution_database.assert_called_once()


def test_cached_action_auto_names_do_not_overwrite(
    mock_database_context, tmp_path, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    terminal = Terminal(mock_database_context)
    terminal._output_cache = MagicMock()
    terminal._output_cache.get_rows.return_value = [{"Database": "msdb"}]
    terminal._output_cache.get_mtime.return_value = None
    previous_format = OutputFormatter.current_format()

    terminal._dispatch_input("!databases --save --format json")
    terminal._dispatch_input("!databases --format=json --save")

    assert json.loads((tmp_path / "databases.json").read_text()) == [{"Database": "msdb"}]
    assert (tmp_path / "databases_2.json").read_text() == (tmp_path / "databases.json").read_text()
    assert OutputFormatter.current_format() == previous_format
    mock_database_context.query_service.execute_table.assert_not_called()
    assert "msdb" in capsys.readouterr().out


def test_bare_save_before_sql_uses_query_name(mock_database_context, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    statement = "EXEC msdb.dbo.rds_task_status;"
    mock_database_context.query_service.execute_table.return_value = [{"task_id": 42}]

    Terminal(mock_database_context)._dispatch_input(f"!sql --format json --save {statement}")

    path = tmp_path / "query_EXEC_msdb.dbo.rds_task_status.json"
    assert json.loads(path.read_text()) == [{"task_id": 42}]
    mock_database_context.query_service.execute_table.assert_called_once_with(statement)


def test_payload_flags_and_whitespace_are_not_consumed():
    sql = "SELECT '--save x --format csv' AS value; -- don't parse this comment"
    raw, options = command_output.extract_options(
        "--format json -- " + sql, action=ActionFactory.get_action("query"), query=True
    )
    assert raw == sql
    assert options == command_output.OutputOptions(format_name="json")

    payload = r'cmd /c "echo --save  C:\temp\a & echo --format json"'
    raw, options = command_output.extract_options(
        "--save=command.txt " + payload, action=ActionFactory.get_action("exec")
    )
    assert raw == payload
    assert options.save == "command.txt"


def test_ad_users_style_and_global_format_are_independent():
    action = ActionFactory.get_action("ad-users")
    raw, options = command_output.extract_options("--style bash --format csv --save", action=action)
    action.validate_arguments(raw)
    assert action._bash_output is True
    assert options == command_output.OutputOptions(save="", format_name="csv")


def test_invalid_options_do_not_execute_or_truncate(mock_database_context, tmp_path):
    path = tmp_path / "existing.txt"
    path.write_text("keep me", encoding="utf-8")
    terminal = Terminal(mock_database_context)
    previous_format = OutputFormatter.current_format()

    terminal._dispatch_input(f'!query --save="{path}" --format invalid SELECT 1')
    terminal._dispatch_input(f'!query --save "{path}" --format= SELECT 1')
    terminal._dispatch_input(f"!query --save={tmp_path / 'missing' / 'output.txt'} SELECT 1")

    assert path.read_text(encoding="utf-8") == "keep me"
    assert OutputFormatter.current_format() == previous_format
    mock_database_context.query_service.execute_table.assert_not_called()


def test_context_restores_stdout_and_format_after_failure(tmp_path):
    path = tmp_path / "partial.txt"
    original_stdout = sys.stdout
    previous_format = OutputFormatter.current_format()

    with pytest.raises(RuntimeError, match="failed"):
        with command_output.apply(command_output.OutputOptions(str(path), "csv"), "query"):
            assert OutputFormatter.current_format() == "csv"
            raise RuntimeError("failed")

    assert sys.stdout is original_stdout
    assert OutputFormatter.current_format() == previous_format


def test_session_file_collects_commands_once(mock_database_context, tmp_path):
    path = tmp_path / "session.txt"
    mock_database_context.query_service.execute_table.return_value = [{"value": "first"}]
    terminal = Terminal(
        mock_database_context,
        output_options=command_output.OutputOptions(save=str(path), append=True),
    )
    terminal._dispatch_input("SELECT 1")
    mock_database_context.query_service.execute_table.return_value = [{"value": "second"}]
    terminal._dispatch_input("!query SELECT 2")
    saved = path.read_text()
    assert saved.count("first") == 1
    assert saved.count("second") == 1


@pytest.mark.parametrize("inline", [False, True])
def test_cli_quoted_output_paths_are_preserved(inline):
    filename = r"C:\reports\user's data.csv"
    arguments = [f"--save={filename}"] if inline else ["--save", filename]
    arguments += ["--format", "csv", "SELECT '--save literal'"]
    raw, options = command_output.extract_options(
        shlex.join(arguments), action=ActionFactory.get_action("query"), query=True,
        shell_quoted=True,
    )
    assert shlex.split(raw) == ["SELECT '--save literal'"]
    assert options.save == filename
    assert options.format_name == "csv"


@pytest.mark.parametrize("action_mode", [False, True])
def test_cli_query_saves_output(mock_database_context, tmp_path, monkeypatch, action_mode):
    path = tmp_path / "cli result.json"
    statement = "SELECT '--save literal' AS value"
    mock_database_context.pre_impersonation_system = "sa"
    mock_database_context.pre_impersonation_user = "dbo"
    mock_database_context.query_service.execute_table.return_value = [{"value": "result"}]
    args = ["mssqlclient-ng", "SQL01", "-u", "sa", "-p", "secret"]
    if action_mode:
        args += ["-a", "query", "--save", str(path), "--format", "json", statement]
    else:
        args += ["--save", str(path), "--format", "json", "-q", statement]
    monkeypatch.setattr(sys, "argv", args)

    with patch.object(cli, "AuthenticationService"), patch.object(
        cli, "DatabaseContext", return_value=mock_database_context
    ), patch.object(cli.logbook, "setup_logging"), patch.object(
        cli.banner, "display_banner", return_value=""
    ):
        assert cli.main() == 0

    assert json.loads(path.read_text()) == [{"value": "result"}]
    mock_database_context.query_service.execute_table.assert_called_once_with(statement)
