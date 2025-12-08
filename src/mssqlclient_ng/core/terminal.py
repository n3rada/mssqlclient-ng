# mssqlclient_ng/core/terminal.py

# Built-in imports
import shlex
import os
from pathlib import Path
import tempfile
from typing import List, Optional

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

from .services.database import DatabaseContext

from .actions.factory import ActionFactory
from .actions.execution import query


SQL_STYLE = style_from_pygments_cls(MonokaiStyle)


class Terminal:
    def __init__(
        self,
        database_context: DatabaseContext,
        log_level: str = "INFO",
    ):

        self.__database_context = database_context
        self.__log_level = log_level

        # Store original user information for restoration after unlinking
        self.__original_mapped_user = database_context.server.mapped_user
        self.__original_system_user = database_context.server.system_user
        self.__original_execution_server = (
            database_context.query_service.execution_server
        )

    def __prompt(self) -> str:
        """
        Build a rich prompt with server, user, and database information.

        Format: [user@server:database]>
        With indicators for sysadmin (*) and impersonation (→)
        """
        server = self.__database_context.server
        user_service = self.__database_context.user_service

        # Get hostname (execution server)
        hostname = self.__database_context.query_service.execution_server

        # Get current database
        database = server.database or "master"

        # Get user information
        mapped_user = user_service.mapped_user or "unknown"
        system_user = user_service.system_user or "unknown"

        # Build the prompt
        prompt_str = f"[{system_user}({mapped_user})@{hostname}:{database}]> "

        return prompt_str

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

        try:
            action.validate_arguments(additional_arguments=" ".join(argument_list))
        except ValueError as ve:
            logger.error(f"Argument validation error: {ve}")
            return None

        # Get server name from database context
        server_name = self.__database_context.query_service.execution_server

        logger.info(f"Executing action '{action_name}' against {server_name}")

        try:
            result = action.execute(database_context=self.__database_context)
            return result
        except KeyboardInterrupt:
            print("\r", end="", flush=True)  # Clear the ^C
            logger.warning("Keyboard interruption received during action execution.")
            return None
        except Exception as e:
            logger.error(f"Error executing action '{action_name}': {e}")
            return None

    def start(
        self,
        prefix: str = "!",
        multiline: bool = False,
        history: bool = False,
    ) -> None:

        if history:
            # Create temp directory for history files
            self.__temp_dir = Path(tempfile.gettempdir()) / "mssqlclient_ng"
            self.__temp_dir.mkdir(exist_ok=True)

            # Create unique history file using hostname
            self.__history_file = (
                self.__temp_dir
                / f"{self.__database_context.server.hostname}_{self.__database_context.server.system_user}_history"
            )

            # Create the history file first if it doesn't exist
            self.__history_file.touch(exist_ok=True)

            # Set permissions to 0600 (rw-------)
            try:
                os.chmod(self.__history_file, 0o600)
            except PermissionError as e:
                logger.warning(
                    f"⚠️ Could not set secure permissions on history file: {e}"
                )

            history_backend = ThreadedHistory(FileHistory(str(self.__history_file)))
            logger.info("💾 Persistent command history enabled.")
        else:
            logger.debug("🗑️ In-memory command history enabled.")
            history_backend = ThreadedHistory(InMemoryHistory())  # in-memory history

        user_input = ""

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

        while True:
            try:
                user_input = prompt_session.prompt(message=self.__prompt())
                if not user_input:
                    continue
            except EOFError:
                break  # Control-D pressed.
            except KeyboardInterrupt:

                if prompt_session.app.current_buffer.text:
                    # If there's text in the buffer, just clear it and continue
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
                    # Execute query without prefix
                    query_action = query.Query()

                    try:
                        query_action.validate_arguments(additional_arguments=user_input)
                    except ValueError as ve:
                        logger.error(f"Argument validation error: {ve}")
                        continue

                    try:
                        query_action.execute(database_context=self.__database_context)
                    except KeyboardInterrupt:
                        print("\r", end="", flush=True)  # Clear the ^C
                        logger.warning(
                            "Keyboard interruption received during remote command execution."
                        )
                    continue

                # Process action command
                command_line = user_input[len(prefix) :].strip()

                if not command_line:
                    continue

                if command_line == "debug":
                    # Toggle debug mode
                    if self.__log_level == "DEBUG":
                        self.__log_level = "INFO"
                        logbook.setup_logging(self.__log_level)
                        logger.info("🔇 Debug mode disabled")
                    else:
                        self.__log_level = "DEBUG"
                        logbook.setup_logging(self.__log_level)
                        logger.info("🔊 Debug mode enabled")

                    continue

                if command_line.startswith("format"):
                    # Handle format command: !format <format_name>
                    parts = command_line.split(maxsplit=1)
                    if len(parts) == 1:
                        # No format specified, show current format and available formats
                        available_formats = ", ".join(
                            OutputFormatter.get_available_formats()
                        )
                        logger.info(
                            f"Current format: {OutputFormatter.current_format()}"
                        )
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

                    continue

                if command_line == "link" or command_line.startswith("link "):
                    # Handle link command: !link <server>[:<user>][,<server>[:<user>]]...
                    parts = command_line.split(maxsplit=1)
                    if len(parts) == 1:
                        # No server specified, show current linked server chain
                        if (
                            self.__database_context.query_service.linked_servers.is_empty
                        ):
                            logger.info("No linked servers currently configured")
                        else:
                            chain_parts = (
                                self.__database_context.query_service.linked_servers.get_chain_parts()
                            )
                            logger.info(
                                f"Current linked server chain: {' -> '.join(chain_parts)}"
                            )
                    else:
                        # Parse and set linked servers
                        link_spec = parts[1]
                        try:

                            # Clear existing chain
                            self.__database_context.query_service.linked_servers.clear()

                            # Parse the link specification: server1,server2:user,server3
                            for server_spec in link_spec.split(","):
                                server_spec = server_spec.strip()
                                if ":" in server_spec:
                                    server_name, user = server_spec.split(":", 1)
                                    server = Server(
                                        hostname=server_name.strip(),
                                        impersonation_user=user.strip(),
                                    )
                                else:
                                    server = Server(hostname=server_spec)

                                self.__database_context.query_service.linked_servers.add_to_chain(
                                    server.hostname
                                )

                                # Handle impersonation if specified
                                if server.impersonation_user:
                                    if self.__database_context.user_service.can_impersonate(
                                        server.impersonation_user
                                    ):
                                        self.__database_context.user_service.impersonate_user(
                                            server.impersonation_user
                                        )
                                        logger.success(
                                            f"Impersonated user: {server.impersonation_user}"
                                        )
                                    else:
                                        logger.warning(
                                            f"Cannot impersonate user: {server.impersonation_user}"
                                        )

                            # Update execution server
                            last_server = self.__database_context.query_service.linked_servers.server_chain[
                                -1
                            ]
                            self.__database_context.query_service.execution_server = (
                                last_server.hostname
                            )

                            # Compute execution database after linked server chain is set up
                            self.__database_context.query_service.compute_execution_database()

                            # Get user info from the final server in the chain
                            try:
                                user_name, system_user = (
                                    self.__database_context.user_service.get_info()
                                )
                                self.__database_context.server.mapped_user = user_name
                                self.__database_context.server.system_user = system_user

                                logger.info(
                                    f"Logged in on {self.__database_context.query_service.execution_server} as {system_user}"
                                )
                                logger.info(f"Mapped to the user: {user_name}")
                            except Exception as exc:
                                logger.error(
                                    f"Error retrieving user info from linked server: {exc}"
                                )

                            chain_parts = (
                                self.__database_context.query_service.linked_servers.get_chain_parts()
                            )
                            logger.success(
                                f"Linked server chain set: {' -> '.join(chain_parts)}"
                            )

                        except Exception as e:
                            logger.error(f"Failed to set linked servers: {e}")

                    continue

                if command_line == "unlink":
                    # Clear linked server chain and revert impersonations
                    if self.__database_context.query_service.linked_servers.is_empty:
                        logger.info("No linked servers to remove")
                    else:
                        # Revert any impersonations
                        # Note: REVERT is idempotent - calling it when not impersonated is safe
                        # We call it once to revert any active impersonation from the linked server chain
                        self.__database_context.user_service.revert_impersonation()

                        # Clear the chain
                        self.__database_context.query_service.linked_servers.clear()

                        # Reset execution server to original
                        self.__database_context.query_service.execution_server = (
                            self.__original_execution_server
                        )

                        # Restore original user info (don't re-query)
                        self.__database_context.server.mapped_user = (
                            self.__original_mapped_user
                        )
                        self.__database_context.server.system_user = (
                            self.__original_system_user
                        )

                        logger.success("Linked server chain cleared")

                    continue

                if command_line.startswith("add-link "):
                    # Add a server to the existing chain: !add-link <server>[:<user>]
                    parts = command_line.split(maxsplit=1)
                    if len(parts) < 2:
                        logger.error("Usage: !add-link <server>[:<user>]")
                        continue

                    server_spec = parts[1].strip()
                    try:
                        # Parse server specification
                        if ":" in server_spec:
                            server_name, user = server_spec.split(":", 1)
                            server = Server(
                                hostname=server_name.strip(),
                                impersonation_user=user.strip(),
                            )
                        else:
                            server = Server(hostname=server_spec)

                        # Add to existing chain
                        self.__database_context.query_service.linked_servers.add_to_chain(
                            server.hostname
                        )

                        # Handle impersonation if specified
                        if server.impersonation_user:
                            if self.__database_context.user_service.can_impersonate(
                                server.impersonation_user
                            ):
                                self.__database_context.user_service.impersonate_user(
                                    server.impersonation_user
                                )
                                logger.success(
                                    f"Impersonated user: {server.impersonation_user}"
                                )
                            else:
                                logger.warning(
                                    f"Cannot impersonate user: {server.impersonation_user}"
                                )
                                # Remove the server we just added since impersonation failed
                                self.__database_context.query_service.linked_servers.server_chain.pop()
                                self.__database_context.query_service.linked_servers._recompute_chain()
                                continue

                        # Update execution server
                        last_server = self.__database_context.query_service.linked_servers.server_chain[
                            -1
                        ]
                        self.__database_context.query_service.execution_server = (
                            last_server.hostname
                        )

                        # Compute execution database
                        self.__database_context.query_service.compute_execution_database()

                        # Get user info from the new server
                        try:
                            user_name, system_user = (
                                self.__database_context.user_service.get_info()
                            )
                            self.__database_context.server.mapped_user = user_name
                            self.__database_context.server.system_user = system_user

                            logger.info(
                                f"Logged in on {self.__database_context.query_service.execution_server} as {system_user}"
                            )
                            logger.info(f"Mapped to the user: {user_name}")
                        except Exception as exc:
                            logger.error(
                                f"Error retrieving user info from linked server: {exc}"
                            )
                            # Rollback the addition
                            self.__database_context.query_service.linked_servers.server_chain.pop()
                            self.__database_context.query_service.linked_servers._recompute_chain()
                            continue

                        chain_parts = (
                            self.__database_context.query_service.linked_servers.get_chain_parts()
                        )
                        logger.success(f"Added to chain: {' -> '.join(chain_parts)}")

                    except Exception as e:
                        logger.error(f"Failed to add linked server: {e}")

                    continue

                if command_line == "pop-link":
                    # Pop the last server from the linked server chain
                    if self.__database_context.query_service.linked_servers.is_empty:
                        logger.info("Already at the original server, cannot go back")
                    elif (
                        len(
                            self.__database_context.query_service.linked_servers.server_chain
                        )
                        == 1
                    ):
                        # Going back from a single-server chain means unlinking completely
                        self.__database_context.user_service.revert_impersonation()
                        self.__database_context.query_service.linked_servers.clear()
                        self.__database_context.query_service.execution_server = (
                            self.__original_execution_server
                        )
                        self.__database_context.server.mapped_user = (
                            self.__original_mapped_user
                        )
                        self.__database_context.server.system_user = (
                            self.__original_system_user
                        )
                        logger.success("Returned to original server")
                    else:
                        # Remove the last server from chain
                        removed_server = (
                            self.__database_context.query_service.linked_servers.server_chain.pop()
                        )
                        self.__database_context.query_service.linked_servers._recompute_chain()

                        # Revert impersonation (if any was used for that server)
                        if removed_server.impersonation_user:
                            self.__database_context.user_service.revert_impersonation()

                        # Update execution server to the new last server
                        last_server = self.__database_context.query_service.linked_servers.server_chain[
                            -1
                        ]
                        self.__database_context.query_service.execution_server = (
                            last_server.hostname
                        )

                        # Compute execution database
                        self.__database_context.query_service.compute_execution_database()

                        # Get user info from the previous server
                        try:
                            user_name, system_user = (
                                self.__database_context.user_service.get_info()
                            )
                            self.__database_context.server.mapped_user = user_name
                            self.__database_context.server.system_user = system_user

                            logger.info(
                                f"Returned to {self.__database_context.query_service.execution_server} as {system_user}"
                            )
                            logger.info(f"Mapped to the user: {user_name}")
                        except Exception as exc:
                            logger.error(f"Error retrieving user info: {exc}")

                        chain_parts = (
                            self.__database_context.query_service.linked_servers.get_chain_parts()
                        )
                        logger.success(f"Current chain: {' -> '.join(chain_parts)}")

                    continue

                action_name, *args = shlex.split(command_line)

                self.execute_action(action_name, args)
