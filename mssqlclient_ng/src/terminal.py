# Built-in imports
import shlex
import os
import re
from pathlib import Path
import tempfile
from typing import Iterable

# External library imports
from loguru import logger
from prompt_toolkit import PromptSession
from prompt_toolkit.auto_suggest import ThreadedAutoSuggest, AutoSuggestFromHistory
from prompt_toolkit.history import ThreadedHistory, InMemoryHistory, FileHistory
from prompt_toolkit.cursor_shapes import CursorShape
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.document import Document

# Local library imports
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.actions.factory import ActionFactory

class Terminal:
    def __init__(self, database_context: DatabaseContext, prefix="!"):

        self.__database_context = database_context

        # Create prompt session with completer
        self.__prompt_session = PromptSession(
            cursor=CursorShape.BLINKING_BEAM,
            multiline=False,
            enable_history_search=True,
            wrap_lines=True,
            auto_suggest=ThreadedAutoSuggest(auto_suggest=AutoSuggestFromHistory()),
            history=ThreadedHistory(InMemoryHistory()),
        )

        self.__prefix = prefix

    def __prompt(self) -> str:
        """
        Build a rich prompt with server, user, and database information.

        Format: [user@server:database]>
        With indicators for sysadmin (*) and impersonation (→)
        """
        server = self.__database_context.server
        user_service = self.__database_context.user_service

        # Get hostname (execution server)
        hostname = server.hostname or "unknown"

        # Get current database
        database = server.database or "master"

        # Get user information
        mapped_user = user_service.mapped_user or "unknown"
        system_user = user_service.system_user or "unknown"

        # Check if user is sysadmin
        is_admin = user_service.is_admin()
        admin_indicator = "*" if is_admin else ""

        # Build the prompt
        prompt_str = f"[{system_user}({mapped_user}){admin_indicator}@{hostname}:{database}]> "

        return prompt_str



    def start(self) -> None:
        result = None
        user_input = ""

        while True:
            try:
                user_input = self.__prompt_session.prompt(message=self.__prompt())
                if not user_input:
                    continue
            except KeyboardInterrupt:

                if self.__prompt_session.app.current_buffer.text:
                    # If there's text in the buffer, just clear it and continue
                    continue

                logger.warning("Keyboard interrupt detected. Exiting terminal.")
                break
            except Exception as exc:
                logger.warning(f"Exception occured: {exc}")
                continue
            else:
                if not user_input.startswith(self.__prefix):
                    action = ActionFactory.get_action("query", user_input)
                    try:
                        action.execute(database_context=self.__database_context)
                    except KeyboardInterrupt:
                        print("\r", end="", flush=True)  # Clear the ^C
                        logger.warning(
                            "Keyboard interruption received during remote command execution."
                        )
                    continue


