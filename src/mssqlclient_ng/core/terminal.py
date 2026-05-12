# mssqlclient_ng/core/terminal.py

# Built-in imports
import shlex
import os
from pathlib import Path
from typing import Callable, Dict, List, Optional

# External library imports
from loguru import logger

from prompt_toolkit import PromptSession
from prompt_toolkit.auto_suggest import ThreadedAutoSuggest, AutoSuggestFromHistory
from prompt_toolkit.history import ThreadedHistory, InMemoryHistory, FileHistory
from prompt_toolkit.cursor_shapes import CursorShape
from prompt_toolkit.completion import merge_completers
from prompt_toolkit.styles import style_from_pygments_cls
from prompt_toolkit.lexers import PygmentsLexer

from pygments.lexers.sql import SqlLexer
from pygments.styles.monokai import MonokaiStyle

# Local library imports
from .utils import logbook
from .utils.common import yes_no_prompt
from .utils.completions import ActionCompleter, SQLBuiltinCompleter
from .utils.formatters import OutputFormatter

from .models.server import Server
from .models.linked_servers import LinkedServers

from .services.database import DatabaseContext

from .actions.factory import ActionFactory
from .actions.execution import query

SQL_STYLE = style_from_pygments_cls(MonokaiStyle)


class Terminal:

    # Aliases for built-in terminal commands
    _BUILTIN_ALIASES = {
        "h": "help",
        "imp": "impersonate",
        "rev": "revert",
        "ul": "unlink",
        "ula": "unlink-all",
        "al": "add-link",
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
        }

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
        self._database_context.server.mapped_user = self._original_mapped_user
        self._database_context.server.system_user = self._original_system_user

    def _update_execution_context(self) -> None:
        """Update execution server/database to match the last server in the chain."""
        last_server = self._database_context.query_service.linked_servers.server_chain[
            -1
        ]
        self._database_context.query_service.execution_server = last_server.hostname
        self._database_context.query_service.compute_execution_database()

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

        try:
            # Use shlex.join to preserve arguments with spaces
            args_str = shlex.join(argument_list)
            action.validate_arguments(args_str)
        except ValueError as ve:
            logger.error(f"Argument validation error: {ve}")
            return None

        # Get server name from database context
        server_name = self._database_context.query_service.execution_server

        logger.info(f"Executing action '{action_name}' against {server_name}")

        try:
            result = action.execute(database_context=self._database_context)
            return result
        except KeyboardInterrupt:
            print("\r", end="", flush=True)  # Clear the ^C
            logger.warning("Keyboard interruption received during action execution.")
            return None
        except Exception as e:
            logger.error(f"Error executing action '{action_name}': {e}")
            return None

    # ── Main Loop ───────────────────────────────────────────────────────

    def start(
        self,
        prefix: str = "!",
        multiline: bool = False,
        history: bool = False,
    ) -> None:

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

            # Create unique history file using hostname and user
            history_file = (
                history_dir
                / f"{self._database_context.server.hostname}_{self._database_context.server.system_user}_history"
            )

            # Create the history file first if it doesn't exist
            history_file.touch(exist_ok=True)

            # Set permissions to 0600 (rw-------)
            try:
                os.chmod(history_file, 0o600)
            except PermissionError as e:
                logger.warning(
                    f"⚠️ Could not set secure permissions on history file: {e}"
                )

            history_backend = ThreadedHistory(FileHistory(str(history_file)))
            logger.info(f"💾 Persistent command history: {history_file}")
        else:
            logger.debug("🗑️ In-memory command history enabled.")
            history_backend = ThreadedHistory(InMemoryHistory())

        if multiline:
            logger.warning(
                "Multiline input mode enabled in terminal, use ESC + ENTER to submit."
            )

        # Merge action completer and SQL builtin completer
        combined_completer = merge_completers(
            [ActionCompleter(prefix=prefix), SQLBuiltinCompleter()]
        )

        prompt_session = PromptSession(
            cursor=CursorShape.BLINKING_BEAM,
            multiline=multiline,
            enable_history_search=True,
            wrap_lines=True,
            auto_suggest=ThreadedAutoSuggest(auto_suggest=AutoSuggestFromHistory()),
            history=history_backend,
            completer=combined_completer,
            lexer=PygmentsLexer(SqlLexer),
            style=SQL_STYLE,
        )

        logger.info(
            f"Type SQL directly or use '{prefix}<action> [args]' to run an action."
        )
        logger.info(
            f"'{prefix}help' lists all actions, '{prefix}<action> --help' shows usage."
        )

        while True:
            try:
                user_input = prompt_session.prompt(message=self._prompt())
                if not user_input:
                    continue
            except EOFError:
                break  # Control-D pressed.
            except KeyboardInterrupt:
                if prompt_session.app.current_buffer.text:
                    continue

                logger.warning("Keyboard interrupt detected.")
                if yes_no_prompt("Exit?", default=True):
                    logger.info("Exiting terminal.")
                    break
                else:
                    continue
            except Exception as exc:
                logger.warning(f"Exception occured: {exc}")
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

        # Build reverse alias map: canonical_name -> [alias, ...]
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

    def _handle_debug(self, command_line: str) -> None:
        """Toggle debug mode."""
        if self._log_level == "DEBUG":
            self._log_level = "INFO"
            logbook.setup_logging(self._log_level)
            logger.info("🔇 Debug mode disabled")
        else:
            self._log_level = "DEBUG"
            logbook.setup_logging(self._log_level)
            logger.info("🔊 Debug mode enabled")

    def _handle_chain(self, command_line: str) -> None:
        """Display full connection chain with impersonation context."""
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
        """Handle link command: !link [server_spec | #id]"""
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

        # Support #<id> to reference a saved chain by its table index
        if link_spec.startswith("#"):
            self._handle_link_by_id(link_spec[1:])
            return

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

            chain_parts = (
                self._database_context.query_service.linked_servers.get_chain_parts()
            )
            logger.success(f"Linked server chain set: {' -> '.join(chain_parts)}")
            logger.info("Use !unlink to go back")

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

            chain_parts = (
                self._database_context.query_service.linked_servers.get_chain_parts()
            )
            logger.success(f"Chain #{chain_id} applied: {' -> '.join(chain_parts)}")
            logger.info("Use !unlink-all to revert")

        except Exception as e:
            logger.error(f"Failed to apply chain #{chain_id}: {e}")

    def _handle_unlink_all(self, command_line: str) -> None:
        """Clear entire linked server chain and revert impersonations."""
        if self._database_context.query_service.linked_servers.is_empty:
            logger.info("No linked servers to remove")
        else:
            self._restore_to_original()
            logger.success("Linked server chain cleared")

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

    def _handle_revert(self, command_line: str) -> None:
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

        except Exception as e:
            logger.error(f"Failed to add linked server: {e}")

    def _handle_unlink(self, command_line: str) -> None:
        """Pop the last server from the linked server chain."""
        linked = self._database_context.query_service.linked_servers

        if linked.is_empty:
            logger.info("Already at the original server, cannot go back")
        elif len(linked.server_chain) == 1:
            # Going back from a single-server chain means unlinking completely
            self._restore_to_original()
            logger.success("Returned to original server")
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

            chain_parts = linked.get_chain_parts()
            logger.success(f"Current chain: {' -> '.join(chain_parts)}")

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
