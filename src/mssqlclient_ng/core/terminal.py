# mssqlclient_ng/core/terminal.py

# Built-in imports
import io
import os
import shlex
import sys
from pathlib import Path
from typing import Callable, Dict, List, Optional

# External library imports
from impacket.tds import SQLErrorException
from loguru import logger

from prompt_toolkit import PromptSession
from prompt_toolkit.auto_suggest import ThreadedAutoSuggest, AutoSuggestFromHistory
from prompt_toolkit.history import ThreadedHistory, InMemoryHistory, FileHistory
from prompt_toolkit.cursor_shapes import CursorShape
from prompt_toolkit.completion import merge_completers
from prompt_toolkit.styles import style_from_pygments_cls
from prompt_toolkit.lexers import PygmentsLexer

from pygments.lexers.sql import SqlLexer
from pygments.styles import get_style_by_name

# Local library imports
from .utils import logbook
from .utils.common import yes_no_prompt
from .utils.completions import (
    ActionCompleter,
    SQLBuiltinCompleter,
    TSQL_STARTERS,
    TSQL_VALID_STANDALONE,
)
from .utils.formatters import OutputFormatter
from .utils.storage import OutputCache

from .models.server import Server
from .models.linked_servers import LinkedServers
from .models.server_execution_state import ServerExecutionState

from .services.database import DatabaseContext

from .actions.factory import ActionFactory
from .actions.execution import query

SQL_STYLE = style_from_pygments_cls(get_style_by_name("one-dark"))


class _TeeWriter:
    """Write to two streams simultaneously (for stdout capture)."""

    def __init__(self, primary, secondary):
        self._primary = primary
        self._secondary = secondary

    def write(self, data):
        self._primary.write(data)
        self._secondary.write(data)

    def flush(self):
        self._primary.flush()
        self._secondary.flush()


class Terminal:

    # Aliases for built-in terminal commands
    _BUILTIN_ALIASES = {
        "h": "help",
        "imp": "impersonate",
        "rev": "revert",
        "ul": "unlink",
        "ula": "unlink-all",
        "al": "add-link",
        "ch": "chain",
    }

    def __init__(
        self,
        database_context: DatabaseContext,
        log_level: str = "INFO",
    ):

        self._database_context = database_context
        self._log_level = log_level

        # Store original user information for restoration after unlinking
        self._original_mapped_user = database_context.server.mapped_user
        self._original_system_user = database_context.server.system_user
        self._original_execution_server = (
            database_context.query_service.execution_server
        )
        self._original_execution_database = (
            database_context.query_service.execution_database
        )

        # Built-in command dispatch table: command_name -> handler(command_line)
        # Handlers return True to continue the loop, False is unused (all continue).
        self._command_handlers: Dict[str, Callable[[str], None]] = {
            "help": self._handle_help,
            "debug": self._handle_debug,
            "chain": self._handle_chain,
            "format": self._handle_format,
            "link": self._handle_link,
            "unlink-all": self._handle_unlink_all,
            "impersonate": self._handle_impersonate,
            "revert": self._handle_revert,
            "add-link": self._handle_add_link,
            "unlink": self._handle_unlink,
            "flush": self._handle_flush,
        }

        self._output_cache = OutputCache()
        self._prefix = "!"  # default; overwritten by start()
        self._history_file: Optional[Path] = None
        self._history_dir: Optional[Path] = (
            None  # set by start() when history is enabled
        )
        self._prompt_session: Optional[PromptSession] = None
        self._session_kwargs: dict = (
            {}
        )  # set by start(); used when recreating session on history switch

    # ── Helper Methods ──────────────────────────────────────────────────

    def _refresh_user_info(self) -> None:
        """Fetch current user info from the server and update the local model."""
        user_name, system_user = self._database_context.user_service.get_info()
        self._database_context.server.mapped_user = user_name
        self._database_context.server.system_user = system_user

    def _restore_to_original(self) -> None:
        """Restore execution context and user info to the original connection state."""
        self._database_context.query_service.linked_servers.clear()
        self._database_context.user_service.revert_impersonation()
        self._database_context.query_service.execution_server = (
            self._original_execution_server
        )
        self._database_context.query_service.execution_database = (
            self._original_execution_database
        )
        # Refresh from the live user_service (now that impersonation is reverted)
        # rather than trusting the stale cached original values.
        self._refresh_user_info()

    def _update_execution_context(self) -> None:
        """Update execution server/database to match the last server in the chain."""
        last_server = self._database_context.query_service.linked_servers.server_chain[
            -1
        ]
        self._database_context.query_service.execution_server = last_server.hostname
        self._database_context.query_service.compute_execution_database()

    def _make_session(self, history_backend) -> PromptSession:
        """Create a fresh PromptSession bound to *history_backend*.

        prompt_toolkit creates its Application (and the Buffer inside it) once
        inside PromptSession.__init__ and never recreates it.  Reassigning
        session.history after construction has no effect because the cached
        Buffer already holds the original history reference and has loaded its
        strings into memory.  Creating a new PromptSession is therefore the
        only reliable way to make the up-arrow key reflect a different history
        file after a server hop.

        ThreadedAutoSuggest is also instantiated fresh here because it carries
        internal thread state (_current_suggestion) that must not be shared
        across sessions.
        """
        return PromptSession(
            **self._session_kwargs,
            history=history_backend,
            auto_suggest=ThreadedAutoSuggest(auto_suggest=AutoSuggestFromHistory()),
        )

    def _switch_history(self, server: Optional[str]) -> None:
        """Switch the prompt session's history to the file for the current (server, identity) context.

        Each unique combination of server + login identity gets its own history
        file, so landing on the same server under a different account starts
        with a clean slate automatically.
        """
        if server is None or self._history_dir is None or not self._session_kwargs:
            return
        state = ServerExecutionState(
            hostname=server,
            system_user=self._database_context.user_service.system_user or "",
            mapped_user=self._database_context.user_service.mapped_user or "",
            is_sysadmin=self._database_context.user_service.is_admin(),
        )
        ctx_hash = state.short_hash
        history_file = self._history_dir / f"{server}_{ctx_hash}_history"
        history_file.touch(exist_ok=True)
        try:
            os.chmod(history_file, 0o600)
        except PermissionError as e:
            logger.warning(f"⚠️ Could not set secure permissions on history file: {e}")
        self._history_file = history_file
        self._prompt_session = self._make_session(
            ThreadedHistory(FileHistory(str(history_file)))
        )
        identity = (
            f"{state.system_user}({state.mapped_user})"
            if state.mapped_user
            else state.system_user
        )
        logger.info(f"Session context [{ctx_hash}]: {server} as {identity}")
        logger.debug(f"History file: {history_file}")

    def _log_server_context(self) -> None:
        """Log the current execution server and switch history to it."""
        server = self._database_context.query_service.execution_server
        logger.info(f"Execution server: {server}")
        self._switch_history(server)
        if not self._database_context.query_service.linked_servers.is_empty:
            logger.info(
                "Use !unlink to pop one link, !revert to undo impersonation, or !unlink-all to revert everything"
            )

    def _cache_context(self) -> tuple:
        """Return the current execution context tuple for cache operations."""
        linked = self._database_context.query_service.linked_servers
        chain_spec = linked.get_chain_arguments() if not linked.is_empty else ""
        return (
            self._database_context.query_service.execution_server or "",
            self._database_context.server.system_user or "",
            chain_spec,
            self._database_context.query_service.execution_database or "",
        )

    def _load_chain_completions(self) -> list:
        """
        Load saved chains for the current server and return [(id, summary), ...].
        Called lazily by ActionCompleter on each Tab press.
        """
        from .utils.storage import ChainStore

        store = ChainStore()
        server_name = self._database_context.server.hostname
        saved = store.load(server_name)
        if not saved or not saved.get("chains"):
            return []
        result = []
        for i, row in enumerate(saved["chains"], start=1):
            endpoint = row.get("Endpoint") or row.get("endpoint", "")
            login = row.get("Login") or row.get("login", "")
            roles = row.get("Server Roles") or row.get("server_roles", "")
            hops = row.get("Hops") or row.get("hops", "")
            parts = [endpoint]
            if login:
                parts.append(f"as {login}")
            if roles:
                parts.append(f"[{roles}]")
            if hops:
                parts.append(f"{hops} hops")
            result.append((i, "  ".join(parts)))
        return result

    # ── Prompt ──────────────────────────────────────────────────────────

    def _prompt(self) -> str:
        """
        Build a rich prompt with server, user, and database information.

        Format: [server]/user(mapped_user)@database>
        """
        server = self._database_context.server
        user_service = self._database_context.user_service

        hostname = self._database_context.query_service.execution_server

        database = (
            self._database_context.query_service.execution_database
            or server.database
            or "master"
        )

        system_user = user_service.system_user
        mapped_user = user_service.mapped_user

        if system_user and mapped_user:
            return f"[{hostname}]/{system_user}({mapped_user})@{database}> "
        elif system_user:
            return f"[{hostname}]/{system_user}@{database}> "
        elif mapped_user:
            return f"[{hostname}]/({mapped_user})@{database}> "
        else:
            return f"[{hostname}]@{database}> "

    # ── Action Execution ────────────────────────────────────────────────

    def execute_action(
        self,
        action_name: str,
        argument_list: List[str],
    ) -> Optional[object]:
        """
        Execute an action by its registered name with a list of arguments.

        Checks the output cache before executing. Use --force/-f to bypass
        the cache and re-execute the action (the fresh output replaces the cache).

        Args:
            action_name: The name of the action to execute
            argument_list: List of arguments for the action

        Returns:
            The result of the action's execution, or None on error
        """
        action = ActionFactory.get_action(action_name)
        if action is None:
            logger.error(f"Unknown action: {action_name}")
            return None

        # Check if help is requested
        if "--help" in argument_list or "-h" in argument_list:
            ActionFactory.display_action_help(action_name)
            return None

        # Resolve canonical action name for cache key
        canonical_name = ActionFactory.resolve_alias(action_name)
        cacheable = OutputCache.is_cacheable(canonical_name)

        # For cacheable actions, extract --force/-f as cache bypass
        # For non-cacheable actions, pass --force/-f through to the action
        force = False
        if cacheable:
            filtered_args = []
            for arg in argument_list:
                if arg in ("--force", "-f"):
                    force = True
                else:
                    filtered_args.append(arg)
        else:
            filtered_args = list(argument_list)

        try:
            # Use shlex.join to preserve arguments with spaces
            args_str = shlex.join(filtered_args)
            action.validate_arguments(args_str)
        except ValueError as ve:
            logger.error(f"Argument validation error: {ve}")
            return None

        # Get server name from database context
        server_name = self._database_context.query_service.execution_server

        # Check output cache (unless --force or action is not cacheable)
        if not force and cacheable:
            ctx = self._cache_context()
            cached = self._output_cache.get(
                ctx[0], ctx[1], ctx[2], ctx[3], canonical_name, args_str
            )
            if cached is not None:
                logger.debug(f"Cache hit for '{canonical_name}' on {server_name}")
                print(cached, end="")
                logger.warning("Cached output. Use --force to re-execute.")
                return None

        logger.info(f"Executing action '{action_name}' against {server_name}")

        # Capture stdout for caching
        stdout_capture = io.StringIO() if cacheable else None

        original_stdout = sys.stdout

        try:
            if stdout_capture is not None:
                # Tee stdout: write to both the real stdout and the capture buffer
                sys.stdout = _TeeWriter(original_stdout, stdout_capture)

            try:
                result = action.execute(database_context=self._database_context)
            finally:
                sys.stdout = original_stdout

            # Cache the captured output
            if stdout_capture is not None:
                output = stdout_capture.getvalue()
                if output:
                    ctx = self._cache_context()
                    self._output_cache.put(
                        ctx[0], ctx[1], ctx[2], ctx[3], canonical_name, args_str, output
                    )

            return result
        except KeyboardInterrupt:
            print("\r", end="", flush=True)  # Clear the ^C
            logger.warning("Keyboard interruption received during action execution.")
            return None
        except Exception as e:
            if isinstance(e, SQLErrorException):
                # Already reported by the query layer via printReplies(); re-logging
                # would produce a duplicate error line.
                return None
            logger.error(f"Error executing action '{action_name}': {e}")
            return None

    # ── Main Loop ───────────────────────────────────────────────────────

    def start(
        self,
        prefix: str = "!",
        multiline: bool = False,
        history: bool = False,
    ) -> None:

        self._prefix = prefix

        if history:
            # Store history in XDG_STATE_HOME (or platform equivalent)
            if os.name == "nt":
                base = Path(
                    os.environ.get(
                        "LOCALAPPDATA", str(Path.home() / "AppData" / "Local")
                    )
                )
            else:
                base = Path(
                    os.environ.get(
                        "XDG_STATE_HOME", str(Path.home() / ".local" / "state")
                    )
                )
            history_dir = base / "mssqlclient_ng" / "history"
            history_dir.mkdir(parents=True, exist_ok=True)
            self._history_dir = history_dir
        else:
            logger.debug("🗑️ In-memory command history enabled.")

        if multiline:
            logger.warning(
                "Multiline input mode enabled in terminal, use ESC + ENTER to submit."
            )

        # Merge action completer and SQL builtin completer
        combined_completer = merge_completers(
            [
                ActionCompleter(
                    prefix=prefix, chain_loader=self._load_chain_completions
                ),
                SQLBuiltinCompleter(),
            ]
        )

        # Store session kwargs so _switch_history can recreate the session with
        # a different history backend (prompt_toolkit caches its Application in
        # __init__, so reassigning session.history has no effect at runtime).
        self._session_kwargs = {
            "cursor": CursorShape.BLINKING_BEAM,
            "multiline": multiline,
            "enable_history_search": True,
            "wrap_lines": True,
            "completer": combined_completer,
            "lexer": PygmentsLexer(SqlLexer),
            "style": SQL_STYLE,
        }

        if history:
            # Delegate to _switch_history: it handles file creation, permissions,
            # identity logging, and session construction.
            self._switch_history(self._database_context.query_service.execution_server)
        else:
            self._prompt_session = self._make_session(
                ThreadedHistory(InMemoryHistory())
            )

        logger.info(
            f"Type SQL directly or use '{prefix}<action> [args]' to run an action."
        )
        logger.info(
            f"'{prefix}help' lists all actions, '{prefix}<action> --help' shows usage."
        )

        assert self._prompt_session is not None

        while True:
            try:
                user_input = self._prompt_session.prompt(message=self._prompt())
                if not user_input:
                    continue
            except EOFError:
                break  # Control-D pressed.
            except KeyboardInterrupt:
                if self._prompt_session.app.current_buffer.text:
                    continue

                logger.warning("Keyboard interrupt detected.")
                if yes_no_prompt("Exit?", default=True):
                    logger.info("Exiting terminal.")
                    break
                else:
                    continue
            except Exception as exc:
                logger.warning(f"Exception occurred: {exc}")
                continue
            else:
                if not user_input.startswith(prefix):
                    self._execute_raw_query(user_input)
                    continue

                # Process action command
                command_line = user_input[len(prefix) :].strip()

                if not command_line:
                    continue

                # Resolve built-in command aliases
                parts = command_line.split(maxsplit=1)
                cmd = self._BUILTIN_ALIASES.get(parts[0], parts[0])
                command_line = cmd + command_line[len(parts[0]) :]

                # Dispatch to built-in handler if matched
                handler = self._match_command(command_line)
                if handler:
                    # Check for --help / -h before dispatching
                    _parts = command_line.split()
                    if len(_parts) > 1 and _parts[1] in ("--help", "-h"):
                        doc = (handler.__doc__ or "No description available.").strip()
                        print(f"  {doc}")
                    else:
                        handler(command_line)
                    continue

                # Otherwise dispatch to action system
                action_name, *args = shlex.split(command_line)
                self.execute_action(action_name, args)

    def _match_command(self, command_line: str) -> Optional[Callable[[str], None]]:
        """Match a command line against registered built-in handlers."""
        for cmd, handler in self._command_handlers.items():
            if command_line == cmd or command_line.startswith(cmd + " "):
                return handler
        return None

    def _execute_raw_query(self, user_input: str) -> None:
        """Execute a raw SQL query (input without prefix)."""
        tokens = user_input.strip().split()
        first = tokens[0].lower().rstrip(";")
        if first not in TSQL_STARTERS:
            logger.warning(
                f"'{first}' is not a recognised T-SQL statement. "
                f"Use '{self._prefix}' prefix for actions."
            )
            return

        if len(tokens) == 1 and first not in TSQL_VALID_STANDALONE:
            logger.warning(
                f"Incomplete T-SQL statement: '{first.upper()}' requires more tokens."
            )
            return

        query_action = query.Query()

        try:
            query_action.validate_arguments(additional_arguments=user_input)
        except ValueError as ve:
            logger.error(f"Argument validation error: {ve}")
            return

        try:
            query_action.execute(database_context=self._database_context)
        except KeyboardInterrupt:
            print("\r", end="", flush=True)  # Clear the ^C
            logger.warning(
                "Keyboard interruption received during remote command execution."
            )

    # ── Built-in Command Handlers ───────────────────────────────────────

    def _handle_help(self, command_line: str) -> None:
        """List actions or show help for a specific one: !help [action|term]"""
        parts = command_line.split(maxsplit=1)
        term = parts[1].strip() if len(parts) > 1 else None

        # Exact action name match → delegate to per-action detailed help
        if term and ActionFactory.action_exists(term):
            ActionFactory.display_action_help(term)
            return

        # Build reverse alias map: canonical_name -> [alias, ]
        reverse_aliases: Dict[str, List[str]] = {}
        for alias, canonical in ActionFactory.list_aliases().items():
            reverse_aliases.setdefault(canonical, []).append(alias)

        all_actions = sorted(ActionFactory.list_actions())

        if term:
            term_lower = term.lower()
            all_actions = [
                n
                for n in all_actions
                if term_lower in n
                or term_lower in (ActionFactory.get_action_description(n) or "").lower()
            ]
            if not all_actions:
                logger.warning(f"No actions matching '{term}'")
                return

        print()
        for name in all_actions:
            desc = ActionFactory.get_action_description(name) or ""
            aliases = reverse_aliases.get(name, [])
            alias_str = f" [{', '.join(aliases)}]" if aliases else ""
            print(f"  {name + alias_str:<35}{desc}")
        print()
        logger.info(f"{len(all_actions)} action(s) — use !<action> --help for details")

    def _handle_debug(self, _command_line: str) -> None:
        """Toggle debug mode."""
        if self._log_level == "DEBUG":
            self._log_level = "INFO"
            logbook.setup_logging(self._log_level)
            logger.info("🔇 Debug mode disabled")
        else:
            self._log_level = "DEBUG"
            logbook.setup_logging(self._log_level)
            logger.info("🔊 Debug mode enabled")

    def _handle_flush(self, command_line: str) -> None:
        """Flush cached action outputs: !flush [--all]"""
        parts = command_line.split()
        if len(parts) > 1 and parts[1] in ("--all", "-a"):
            deleted = self._output_cache.flush()
            logger.success(f"Flushed {deleted} cached output(s) across all contexts")
        else:
            ctx = self._cache_context()
            deleted = self._output_cache.flush(ctx[0], ctx[1], ctx[2], ctx[3])
            server = self._database_context.query_service.execution_server
            logger.success(f"Flushed {deleted} cached output(s) for {server}")

    def _handle_chain(self, command_line: str) -> None:
        """Display current chain, or apply a saved linkmap chain by ID: !chain [id]"""
        parts = command_line.split(maxsplit=1)
        if len(parts) > 1:
            id_str = parts[1].strip().lstrip("#")
            self._handle_link_by_id(id_str)
            return
        self._display_chain()

    def _handle_format(self, command_line: str) -> None:
        """Handle format command: !format [format_name]"""
        parts = command_line.split(maxsplit=1)
        if len(parts) == 1:
            available_formats = ", ".join(OutputFormatter.get_available_formats())
            logger.info(f"Current format: {OutputFormatter.current_format()}")
            logger.info(f"Available formats: {available_formats}")
        else:
            format_name = parts[1]
            try:
                OutputFormatter.set_format(format_name)
                logger.success(
                    f"Output format changed to: {OutputFormatter.current_format()}"
                )
            except ValueError as e:
                logger.error(str(e))

    def _handle_link(self, command_line: str) -> None:
        """Hop to a directly linked server: !link [server_spec]"""
        parts = command_line.split(maxsplit=1)
        if len(parts) == 1:
            # No server specified, show current linked server chain
            if self._database_context.query_service.linked_servers.is_empty:
                logger.info("No linked servers currently configured")
            else:
                chain_parts = (
                    self._database_context.query_service.linked_servers.get_chain_parts()
                )
                logger.info(f"Current linked server chain: {' -> '.join(chain_parts)}")
            return

        link_spec = parts[1].strip()
        try:
            new_chain = LinkedServers(link_spec)
            self._database_context.query_service.linked_servers = new_chain

            # Update execution server to last server in chain
            last_server = new_chain.server_chain[-1]
            self._database_context.query_service.execution_server = last_server.hostname

            # Compute execution database after linked server chain is set up
            self._database_context.query_service.compute_execution_database()

            try:
                self._refresh_user_info()
                logger.info(
                    f"Logged in on {self._database_context.query_service.execution_server} as {self._database_context.server.system_user}"
                )
                logger.info(
                    f"Mapped to the user: {self._database_context.server.mapped_user}"
                )
            except Exception as exc:
                logger.error(f"Error retrieving user info from linked server: {exc}")

            chain_display = self._database_context.query_service.linked_servers.format_chain_display(
                initial_host=self._original_execution_server or "",
                initial_login=self._original_system_user,
            )
            logger.success(f"Linked server chain set: {chain_display}")
            self._log_server_context()

        except Exception as e:
            logger.error(f"Failed to set linked servers: {e}")

    def _handle_link_by_id(self, id_str: str) -> None:
        """Look up a saved chain by its # index and apply it."""
        try:
            chain_id = int(id_str)
        except ValueError:
            logger.error(f"Invalid chain ID: #{id_str}")
            return

        from .utils.storage import ChainStore

        store = ChainStore()
        server_name = self._database_context.server.hostname
        saved = store.load(server_name)

        if not saved or not saved.get("chains"):
            logger.error(f"No saved chains for {server_name}. Run !linkmap first.")
            return

        chains = saved["chains"]
        if chain_id < 1 or chain_id > len(chains):
            logger.error(f"Chain #{chain_id} not found (valid: 1-{len(chains)})")
            return

        row = chains[chain_id - 1]
        command = row.get("Command") or row.get("command", "")
        if not command:
            logger.error(f"Chain #{chain_id} has no command")
            return

        # The command is "HOST/imp -l CHAIN_ARG" - extract the -l part
        if " -l " not in command:
            logger.error(f"Cannot parse chain command: {command}")
            return

        link_spec = command.split(" -l ", 1)[1]

        # Also extract host impersonation if present
        host_part = command.split(" -l ", 1)[0].strip().strip('"')
        host_impersonation = []
        if "/" in host_part:
            host_impersonation = host_part.split("/")[1:]

        # Apply host impersonation first
        if host_impersonation:
            for login in host_impersonation:
                if not self._database_context.user_service.impersonate_user(login):
                    logger.error(f"Failed to impersonate '{login}' on starting server")
                    return

        try:
            new_chain = LinkedServers(link_spec)
            self._database_context.query_service.linked_servers = new_chain

            last_server = new_chain.server_chain[-1]
            self._database_context.query_service.execution_server = last_server.hostname
            self._database_context.query_service.compute_execution_database()

            try:
                self._refresh_user_info()
                logger.info(
                    f"Logged in on {self._database_context.query_service.execution_server} "
                    f"as {self._database_context.server.system_user}"
                )
                logger.info(
                    f"Mapped to the user: {self._database_context.server.mapped_user}"
                )
            except Exception as exc:
                logger.error(f"Error retrieving user info from linked server: {exc}")

            chain_display = self._database_context.query_service.linked_servers.format_chain_display(
                initial_host=self._original_execution_server or "",
                initial_login=self._original_system_user,
            )
            logger.success(f"Chain #{chain_id} applied: {chain_display}")
            self._log_server_context()

        except Exception as e:
            logger.error(f"Failed to apply chain #{chain_id}: {e}")

    def _handle_unlink_all(self, _command_line: str) -> None:
        """Clear entire linked server chain and revert impersonations."""
        if self._database_context.query_service.linked_servers.is_empty:
            logger.info("No linked servers to remove")
        else:
            self._restore_to_original()
            logger.success("Linked server chain cleared")
            self._log_server_context()

    def _handle_impersonate(self, command_line: str) -> None:
        """Impersonate a login on the current connection: !impersonate <login>"""
        parts = command_line.split(maxsplit=1)
        if len(parts) < 2:
            logger.error("Usage: !impersonate <login>")
            return

        login = parts[1].strip()
        try:
            if self._database_context.user_service.can_impersonate(login):
                if self._database_context.user_service.impersonate_user(login):
                    self._refresh_user_info()
                    logger.success(
                        f"Impersonated: {self._database_context.server.system_user}"
                    )
                    logger.info("Use !revert to revert impersonation")
                else:
                    logger.error(f"Failed to impersonate: {login}")
            else:
                logger.warning(f"Cannot impersonate: {login}")
        except Exception as e:
            logger.error(f"Error during impersonation: {e}")

    def _handle_revert(self, _command_line: str) -> None:
        """Revert impersonation on the current connection."""
        try:
            self._database_context.user_service.revert_impersonation()
            self._refresh_user_info()
            logger.success(f"Reverted to: {self._database_context.server.system_user}")
        except Exception as e:
            logger.error(f"Error reverting impersonation: {e}")

    def _handle_add_link(self, command_line: str) -> None:
        """Add a server to the existing chain: !add-link <server>[/user][@db]"""
        parts = command_line.split(maxsplit=1)
        if len(parts) < 2:
            logger.error("Usage: !add-link <server>[/user1[/user2]][@db]")
            return

        server_spec = parts[1].strip()
        try:
            server = Server.parse_server(server_spec)

            # Add to existing chain with impersonation users and database
            self._database_context.query_service.linked_servers.add_to_chain(
                server.hostname,
                impersonation_users=server.impersonation_users or None,
                database=server.database,
            )

            self._update_execution_context()

            try:
                self._refresh_user_info()
                logger.info(
                    f"Logged in on {self._database_context.query_service.execution_server} as {self._database_context.server.system_user}"
                )
                logger.info(
                    f"Mapped to the user: {self._database_context.server.mapped_user}"
                )
            except Exception as exc:
                logger.error(f"Error retrieving user info from linked server: {exc}")
                # Rollback the addition
                self._database_context.query_service.linked_servers.remove_last_from_chain()
                return

            chain_parts = (
                self._database_context.query_service.linked_servers.get_chain_parts()
            )
            logger.success(f"Added to chain: {' -> '.join(chain_parts)}")
            self._log_server_context()

        except Exception as e:
            logger.error(f"Failed to add linked server: {e}")

    def _handle_unlink(self, _command_line: str) -> None:
        """Pop the last server from the linked server chain."""
        linked = self._database_context.query_service.linked_servers

        if linked.is_empty:
            logger.warning("Already at the original server, cannot go back")
        elif len(linked.server_chain) == 1:
            # Going back from a single-server chain means unlinking completely
            self._restore_to_original()
            logger.success("Returned to original server")
            self._log_server_context()
        else:
            # Remove the last server from chain
            linked.remove_last_from_chain()

            self._update_execution_context()

            try:
                self._refresh_user_info()
                logger.info(
                    f"Returned to {self._database_context.query_service.execution_server} as {self._database_context.server.system_user}"
                )
                logger.info(
                    f"Mapped to the user: {self._database_context.server.mapped_user}"
                )
            except Exception as exc:
                logger.error(f"Error retrieving user info: {exc}")

            chain_parts = linked.format_chain_display(
                initial_host=self._original_execution_server or "",
                initial_login=self._original_system_user,
            )
            logger.success(f"Current chain: {chain_parts}")
            self._log_server_context()

    # ── Display ─────────────────────────────────────────────────────────

    def _display_chain(self) -> None:
        """Display the full connection chain with impersonation context, MSSQLand style."""
        linked = self._database_context.query_service.linked_servers

        if linked.is_empty:
            current_system = self._database_context.server.system_user
            result = f"{self._original_execution_server} ({self._original_system_user})"
            if current_system and current_system != self._original_system_user:
                result += f" → impersonating {current_system}"
            logger.info(f"Context: {result}")
        else:
            chain_display = linked.format_chain_display(
                initial_host=self._original_execution_server or "",
                initial_login=self._original_system_user,
            )
            logger.info(f"Chain: {chain_display}")
