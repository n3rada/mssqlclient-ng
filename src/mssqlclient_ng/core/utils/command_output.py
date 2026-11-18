"""Common output options for CLI actions and interactive commands."""

# Built-in imports
from __future__ import annotations

import contextlib
import hashlib
import re
import shlex
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

# Third-party imports
from loguru import logger

# Local imports
from .formatters import OutputFormatter

if TYPE_CHECKING:
    from ..actions.base import BaseAction


@dataclass
class OutputOptions:
    """Per-command output settings; an empty save path requests an automatic name."""

    save: str | None = None
    format_name: str | None = None
    append: bool = False

    def override(self, other: "OutputOptions") -> "OutputOptions":
        return OutputOptions(
            save=other.save if other.save is not None else self.save,
            format_name=other.format_name if other.format_name is not None else self.format_name,
            append=self.append if other.save is None else other.append,
        )


class TeeWriter:
    """Write formatted output to the terminal and a second stream."""

    def __init__(self, primary: TextIO, secondary: TextIO) -> None:
        self._primary = primary
        self._secondary = secondary

    def write(self, data: str) -> int:
        written = self._primary.write(data)
        self._secondary.write(data)
        return written

    def flush(self) -> None:
        self._primary.flush()
        self._secondary.flush()


@dataclass(frozen=True)
class _Token:
    raw: str
    start: int
    end: int


def _tokens(text: str) -> Iterator[_Token]:
    """Tokenize without changing quotes, whitespace, or Windows backslashes."""
    index = 0
    while index < len(text):
        if text[index].isspace():
            index += 1
            continue
        start = index
        quote = ""
        while index < len(text):
            char = text[index]
            if quote:
                if char == quote:
                    if index + 1 < len(text) and text[index + 1] == quote:
                        index += 2
                        continue
                    quote = ""
            elif char in ("'", '"'):
                quote = char
            elif char.isspace():
                break
            index += 1
        yield _Token(text[start:index], start, index)


def _value(raw: str, shell_quoted: bool) -> str:
    if shell_quoted:
        values = shlex.split(raw)
        return values[0] if values else ""
    if raw.startswith(("'", '"')):
        if len(raw) < 2 or raw[-1] != raw[0]:
            raise ValueError("Unclosed quote in output option")
        return raw[1:-1]
    return raw


def extract_options(
    raw_args: str,
    *,
    action: BaseAction | None = None,
    query: bool = False,
    shell_quoted: bool = False,
) -> tuple[str, OutputOptions]:
    """Remove common options while preserving the remaining argument text.

    Query options precede the SQL. For actions with a remainder argument,
    options precede that payload. ``--`` explicitly ends common-option parsing.
    """
    fields = action._get_arg_fields() if action is not None else {}
    named = {
        flag: arg
        for arg in fields.values()
        for flag in (
            f"--{arg.long_name}" if arg.long_name else "",
            f"-{arg.short_name}" if arg.short_name else "",
        )
        if flag
    }
    remainder = 0 if query else (
        action._remainder_after
        if action is not None and action._remainder_after is not None
        else action._compute_remainder_after() if action is not None else None
    )
    tokens = list(_tokens(raw_args))
    options = OutputOptions()
    removed: list[tuple[int, int]] = []
    positional = 0
    index = 0
    while index < len(tokens):
        token = tokens[index]
        token_text = _value(token.raw, True) if shell_quoted else token.raw
        flag, equals, inline = token_text.partition("=")
        if token_text == "--":
            removed.append((token.start, token.end))
            break

        if flag in ("--save", "--format", "--output-format"):
            end = token.end
            value = (inline if shell_quoted else _value(inline, False)) if equals else None
            consumed = 1
            following = tokens[index + 1] if index + 1 < len(tokens) else None
            if value is None and following is not None and not following.raw.startswith("-"):
                # A bare --save before SQL must not consume EXEC/SELECT/etc.
                sql_starts_here = False
                if query and flag == "--save":
                    from .completions import TSQL_STARTERS

                    words = _value(following.raw, shell_quoted).split(maxsplit=1)
                    next_word = words[0] if words else ""
                    sql_starts_here = next_word.lower().rstrip(";") in TSQL_STARTERS
                if not sql_starts_here:
                    value = _value(following.raw, shell_quoted)
                    end = following.end
                    consumed = 2

            if flag == "--save":
                options.save = value if value is not None else ""
            else:
                if not value:
                    raise ValueError(f"{flag} requires an output format")
                options.format_name = value
            removed.append((token.start, end))
            index += consumed
            continue

        # Do not interpret common-looking flags inside a declared argument.
        descriptor = named.get(flag)
        if descriptor is not None:
            index += 1
            if not descriptor.toggle and not equals and index < len(tokens):
                if not tokens[index].raw.startswith("-"):
                    index += 1
            continue
        if token_text.startswith("-"):
            if query and token_text != "--all":
                break
            index += 1
            continue
        if remainder is not None and positional >= remainder:
            break
        positional += 1
        index += 1

    remaining = raw_args
    for start, end in reversed(removed):
        remaining = remaining[:start] + remaining[end:]
    return remaining.strip(), options


def automatic_filename(name: str, *, query: bool = False) -> str:
    """Build a portable filename from an action name or SQL text."""
    stem = re.sub(r"[^\w.-]+", "_", name, flags=re.ASCII).strip("._-") or "output"
    if query:
        stem = "query_" + stem
    if len(stem) > 120:
        digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:10]
        stem = f"{stem[:109]}_{digest}"
    if stem.split(".", 1)[0].upper() in {
        "CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10)),
    }:
        stem = "output_" + stem
    extension = {"markdown": "md", "csv": "csv", "json": "json", "grid": "txt"}[
        OutputFormatter.current_format()
    ]
    return f"{stem}.{extension}"


@contextlib.contextmanager
def apply(options: OutputOptions, name: str, *, query: bool = False) -> Iterator[None]:
    """Apply one command's format and optionally tee its output to a file."""
    previous_format = OutputFormatter.current_format()
    output_path: Path | None = None
    try:
        if options.format_name is not None:
            OutputFormatter.set_format(options.format_name)
        with contextlib.ExitStack() as stack:
            if options.save is not None:
                if options.save:
                    output_path = Path(options.save).expanduser()
                    output_file = output_path.open(
                        "a" if options.append else "w", encoding="utf-8", newline=""
                    )
                else:
                    base = Path(automatic_filename(name, query=query))
                    output_path = base
                    suffix = 2
                    while True:
                        try:
                            output_file = output_path.open("x", encoding="utf-8", newline="")
                            break
                        except FileExistsError:
                            output_path = base.with_name(f"{base.stem}_{suffix}{base.suffix}")
                            suffix += 1
                stack.enter_context(output_file)
                stack.enter_context(contextlib.redirect_stdout(TeeWriter(sys.stdout, output_file)))
            yield
        if output_path is not None:
            logger.success(f"Output saved to {output_path}")
    finally:
        if options.format_name is not None:
            OutputFormatter.set_format(previous_format)


def print_help() -> None:
    print("common output options:")
    print("  --save [FILE]            Save output; omit FILE for an automatic name")
    print("  --format <FORMAT>        Format this command: markdown, csv, grid, json")
    print("  --output-format <FORMAT> Alias for --format")
