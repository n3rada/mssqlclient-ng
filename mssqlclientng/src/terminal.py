# Built-in imports
import shlex
from typing import List, Optional

# External library imports
from loguru import logger

from prompt_toolkit import PromptSession
from prompt_toolkit.auto_suggest import ThreadedAutoSuggest, AutoSuggestFromHistory
from prompt_toolkit.history import ThreadedHistory, InMemoryHistory
from prompt_toolkit.cursor_shapes import CursorShape
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.document import Document
from prompt_toolkit.styles import style_from_pygments_cls
from prompt_toolkit.lexers import PygmentsLexer

from pygments.lexers.sql import SqlLexer
from pygments.styles.friendly import FriendlyStyle

# Local library imports
from mssqlclientng.src.services.database import DatabaseContext
from mssqlclientng.src.actions.factory import ActionFactory


SQL_STYLE = style_from_pygments_cls(FriendlyStyle)


class ActionCompleter(Completer):
    """
    Auto-completer for action commands.
    Suggests available actions when user starts typing with prefix.
    """

    def __init__(self, prefix: str = "!"):
        self.prefix = prefix

    def get_completions(self, document: Document, complete_event):
        """
        Generate completion suggestions.

        Args:
            document: The document being edited
            complete_event: The completion event

        Yields:
            Completion objects for matching actions with descriptions
        """
        text = document.text_before_cursor

        # Check if we're at the start with prefix
        if text.startswith(self.prefix):
            # Get the part after the prefix
            command_part = text[len(self.prefix) :].strip()

            # Get all available actions
            actions = ActionFactory.list_actions()

            # Filter actions that match what the user has typed
            for action_name in actions:
                if action_name.startswith(command_part.lower()):
                    # Calculate how much we need to replace
                    completion_text = action_name[len(command_part) :]

                    # Get the description for this action
                    description = ActionFactory.get_action_description(action_name)
                    help_text = f"{description}" if description else ""

                    yield Completion(completion_text, 0, display_meta=help_text)


class Terminal:
    def __init__(
        self,
        database_context: DatabaseContext,
    ):

        self.__database_context = database_context

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
    ) -> None:

        user_input = ""

        if multiline:
            logger.warning(
                "Multiline input mode enabled in terminal, use ESC + ENTER to submit."
            )

        prompt_session = PromptSession(
            cursor=CursorShape.BLINKING_BEAM,
            multiline=multiline,
            enable_history_search=True,
            wrap_lines=True,
            auto_suggest=ThreadedAutoSuggest(auto_suggest=AutoSuggestFromHistory()),
            history=ThreadedHistory(InMemoryHistory()),
            completer=ActionCompleter(prefix=prefix),
            lexer=PygmentsLexer(SqlLexer),
            style=SQL_STYLE,
        )

        while True:
            try:
                user_input = prompt_session.prompt(message=self.__prompt())
                if not user_input:
                    continue
            except KeyboardInterrupt:

                if prompt_session.app.current_buffer.text:
                    # If there's text in the buffer, just clear it and continue
                    continue

                logger.warning("Keyboard interrupt detected. Exiting terminal.")
                break
            except Exception as exc:
                logger.warning(f"Exception occured: {exc}")
                continue
            else:
                if not user_input.startswith(prefix):
                    # Execute query without prefix
                    query_action = ActionFactory.get_action("query")

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

                action_name, *args = shlex.split(command_line)

                self.execute_action(action_name, args)
