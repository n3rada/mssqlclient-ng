# mssqlclient_ng/core/actions/execution/sql_file.py

# Built-in imports
import codecs
import re
from pathlib import Path

# Third-party imports
from impacket.tds import SQLErrorException
from loguru import logger

# Local imports
from ..base import Arg, BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter

# sqlcmd/SSMS batch separator: GO alone on its line, optional repeat count
_GO_LINE = re.compile(r"^\s*go(?:\s+(\d+))?\s*(?:--.*)?$", re.IGNORECASE)

Batch = tuple[int, str, int]  # (starting line, SQL text, repeat count)


def decode_sql_file(data: bytes) -> str:
    """Decode a script saved as UTF-16 (SSMS "Unicode") or UTF-8, with or without BOM."""
    if data.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        return data.decode("utf-16")
    return data.decode("utf-8-sig")


def split_batches(script: str) -> list[Batch]:
    """Split a T-SQL script on GO separator lines, dropping empty batches."""
    batches: list[Batch] = []
    current: list[str] = []
    start_line = 1

    def flush(repeat: int) -> None:
        sql = "\n".join(current).strip()
        if sql:
            batches.append((start_line, sql, repeat))

    for line_number, line in enumerate(script.splitlines(), start=1):
        match = _GO_LINE.match(line)
        if match:
            flush(int(match.group(1)) if match.group(1) else 1)
            current = []
            start_line = line_number + 1
        else:
            current.append(line)

    flush(1)
    return batches


@ActionFactory.register(
    "sql-file",
    "Execute a local T-SQL script file, split into batches on GO lines.",
    aliases=["source"],
)
class SqlFile(BaseAction):
    """
    Execute a local T-SQL script file against the current execution server.

    The script is split on GO separator lines (GO <count> repeats a batch),
    like sqlcmd and SSMS. Each batch is routed through the linked server
    chain when one is set. Execution stops at the first failed batch.

    Through linked servers each batch runs on its own, so a USE does not
    carry to the next batch: pick the database with !use first.

    Any text file works (.sql, .txt, ...) saved as UTF-8 or UTF-16 (with BOM).

    Usage:
        sql-file ./setup.sql
        source ~/scripts/enum.txt
    """

    _path_str = Arg(position=0, required=True, description="Local T-SQL file path")

    def __init__(self):
        super().__init__()
        self._path: Path | None = None
        self._batches: list[Batch] = []

    def validate_arguments(self, additional_arguments: str = "") -> None:
        self._bind_arguments(additional_arguments)

        path = Path(self._path_str).expanduser().resolve()
        if not path.exists():
            raise ValueError(f"Local file does not exist: {path}")
        if not path.is_file():
            raise ValueError(f"Path is not a file: {path}")

        try:
            script = decode_sql_file(path.read_bytes())
        except UnicodeDecodeError as exc:
            raise ValueError(f"{path.name} is not UTF-8 or UTF-16 text: {exc}") from exc

        batches = split_batches(script)
        if not batches:
            raise ValueError(f"No T-SQL found in {path}")

        self._path = path
        self._batches = batches

    def execute(self, database_context: DatabaseContext) -> None:
        assert self._path is not None
        query_service = database_context.query_service
        total = len(self._batches)

        logger.info(
            f"Running {total} batch(es) from {self._path.name} "
            f"against {query_service.execution_server}"
        )

        executed = 0
        try:
            for index, (line, sql, repeat) in enumerate(self._batches, start=1):
                for _ in range(repeat):
                    logger.debug(f"Batch {index}/{total} (line {line}):\n{sql}")
                    try:
                        rows = query_service.execute_table(sql)
                    except Exception as exc:
                        # SQL errors were already printed by the query layer
                        detail = "" if isinstance(exc, SQLErrorException) else f": {exc}"
                        logger.error(f"Batch {index}/{total} (line {line}) failed{detail}")
                        logger.warning(
                            f"Stopped after {executed} of {total} batch(es); "
                            "the remaining batches were not run"
                        )
                        return None

                    if rows:
                        print()
                        print(OutputFormatter.convert_list_of_dicts(rows))
                executed += 1
        finally:
            # A USE inside the script moves the direct session
            query_service.sync_execution_database()

        logger.success(f"Executed {executed} batch(es) from {self._path.name}")
        return None
