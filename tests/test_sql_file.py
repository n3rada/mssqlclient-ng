# tests/test_sql_file.py

"""Tests for the sql-file action and local path completion."""

# Built-in imports
import codecs

# Third-party imports
import pytest
from impacket.tds import SQLErrorException
from prompt_toolkit.completion import CompleteEvent
from prompt_toolkit.document import Document

# Local imports
from mssqlclient_ng.core.actions.execution.sql_file import (
    SqlFile,
    decode_sql_file,
    split_batches,
)
from mssqlclient_ng.core.actions.factory import ActionFactory
from mssqlclient_ng.core.utils.completions import ActionCompleter
from mssqlclient_ng.core.utils.storage import OutputCache


class TestSplitBatches:
    def test_no_separator_is_one_batch(self):
        assert split_batches("SELECT 1;\nSELECT 2;") == [(1, "SELECT 1;\nSELECT 2;", 1)]

    def test_splits_on_go_lines_with_start_lines(self):
        script = "SELECT 1\nGO\n\nSELECT 2\ngo\nSELECT 3"
        assert split_batches(script) == [
            (1, "SELECT 1", 1),
            (3, "SELECT 2", 1),
            (6, "SELECT 3", 1),
        ]

    def test_go_with_count_and_comment(self):
        script = "INSERT INTO t DEFAULT VALUES\n  GO 3  -- repeat\nSELECT 1"
        assert split_batches(script) == [
            (1, "INSERT INTO t DEFAULT VALUES", 3),
            (3, "SELECT 1", 1),
        ]

    def test_go_inside_a_line_is_not_a_separator(self):
        script = "SELECT 'GO' AS go_col\nEXEC sp_who GO_AWAY"
        assert len(split_batches(script)) == 1

    def test_empty_batches_dropped(self):
        assert split_batches("GO\n\nGO\nSELECT 1\nGO\n") == [(4, "SELECT 1", 1)]


class TestDecodeSqlFile:
    def test_utf8_bom(self):
        assert decode_sql_file(codecs.BOM_UTF8 + b"SELECT 1") == "SELECT 1"

    def test_utf16_le_bom(self):
        assert decode_sql_file("SELECT N'é'".encode("utf-16")) == "SELECT N'é'"

    def test_plain_utf8(self):
        assert decode_sql_file("SELECT 'ü'".encode()) == "SELECT 'ü'"


class TestSqlFileValidation:
    def test_registered_with_alias(self):
        assert ActionFactory.resolve_alias("source") == "sql-file"
        assert isinstance(ActionFactory.get_action("source"), SqlFile)

    def test_not_cached(self):
        assert not OutputCache.is_cacheable("sql-file")

    def test_missing_file(self, tmp_path):
        with pytest.raises(ValueError, match="does not exist"):
            SqlFile().validate_arguments((tmp_path / "nope.sql").as_posix())

    def test_directory(self, tmp_path):
        with pytest.raises(ValueError, match="not a file"):
            SqlFile().validate_arguments(tmp_path.as_posix())

    def test_only_separators(self, tmp_path):
        script = tmp_path / "empty.sql"
        script.write_text("GO\n\nGO\n")
        with pytest.raises(ValueError, match="No T-SQL found"):
            SqlFile().validate_arguments(script.as_posix())

    def test_binary_file(self, tmp_path):
        script = tmp_path / "blob.sql"
        script.write_bytes(b"\xff\xfe\x00\xd8")  # UTF-16 BOM + lone surrogate
        with pytest.raises(ValueError):
            SqlFile().validate_arguments(script.as_posix())

    def test_missing_argument(self):
        with pytest.raises(ValueError):
            SqlFile().validate_arguments("")

    def test_any_text_file_accepted(self, tmp_path):
        script = tmp_path / "enum.txt"
        script.write_text("SELECT 1\nGO\nSELECT 2\n")
        action = SqlFile()
        action.validate_arguments(script.as_posix())
        assert action._batches == [(1, "SELECT 1", 1), (3, "SELECT 2", 1)]


class TestSqlFileExecute:
    def _action(self, tmp_path, script: str) -> SqlFile:
        path = tmp_path / "script.sql"
        path.write_text(script)
        action = SqlFile()
        action.validate_arguments(path.as_posix())
        return action

    def test_runs_each_batch_and_repeats(self, tmp_path, mock_database_context):
        action = self._action(tmp_path, "SELECT 1\nGO 2\nSELECT 2")
        qs = mock_database_context.query_service
        qs.execute_table.return_value = []

        action.execute(mock_database_context)

        assert [c.args[0] for c in qs.execute_table.call_args_list] == [
            "SELECT 1",
            "SELECT 1",
            "SELECT 2",
        ]
        qs.sync_execution_database.assert_called_once()

    def test_prints_result_rows(self, tmp_path, mock_database_context, capsys):
        action = self._action(tmp_path, "SELECT name FROM sys.databases")
        mock_database_context.query_service.execute_table.return_value = [
            {"name": "master"}
        ]
        action.execute(mock_database_context)
        assert "master" in capsys.readouterr().out

    @pytest.mark.parametrize(
        "error", [SQLErrorException("bad syntax"), RuntimeError("timeout")]
    )
    def test_stops_on_first_failure(self, tmp_path, mock_database_context, error):
        action = self._action(tmp_path, "SELECT 1\nGO\nSELEC 2\nGO\nSELECT 3")
        qs = mock_database_context.query_service
        qs.execute_table.side_effect = [[], error, []]

        action.execute(mock_database_context)

        assert qs.execute_table.call_count == 2
        qs.sync_execution_database.assert_called_once()


class TestLocalPathCompletion:
    def _complete(self, text: str) -> list[str]:
        completer = ActionCompleter(prefix="!")
        return [
            c.text
            for c in completer.get_completions(Document(text), CompleteEvent())
        ]

    @pytest.fixture
    def scripts_dir(self, tmp_path, monkeypatch):
        (tmp_path / "enum.sql").write_text("SELECT 1")
        (tmp_path / "notes.txt").write_text("x")
        (tmp_path / "sub").mkdir()
        monkeypatch.chdir(tmp_path)
        return tmp_path

    @pytest.mark.parametrize("command", ["!sql-file ", "!source ", "!upload "])
    def test_lists_local_files_and_dirs(self, scripts_dir, command):
        assert sorted(self._complete(command)) == ["enum.sql", "notes.txt", "sub"]

    def test_completes_partial_name(self, scripts_dir):
        assert self._complete("!source en") == ["um.sql"]

    def test_flags_still_complete(self, scripts_dir):
        assert "elp" in self._complete("!sql-file --h")

    def test_second_argument_not_path_completed(self, scripts_dir):
        assert "notes.txt" not in self._complete("!upload enum.sql ")
