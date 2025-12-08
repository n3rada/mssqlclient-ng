# mssqlclient_ng/core/actions/base.py

# Built-in imports
import shlex
import re
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional

# Third party imports
from loguru import logger


class BaseAction(ABC):
    """
    Abstract base class for all actions, enforcing validation and execution logic.
    """

    @abstractmethod
    def validate_arguments(self, additional_arguments: str) -> None:
        pass

    @abstractmethod
    def execute(self, database_context=None) -> Optional[object]:
        pass

    def split_arguments(
        self, additional_arguments: str, separator: str = ","
    ) -> List[str]:
        if not additional_arguments or additional_arguments.strip() == "":
            logger.debug("No arguments provided.")
            return []
        # Use shlex to split respecting quotes
        splitted = [
            arg for arg in shlex.split(additional_arguments) if arg != separator
        ]
        logger.debug(f"Splitted arguments: {splitted}")
        return splitted

    def parse_arguments(
        self, additional_arguments: str
    ) -> Tuple[Dict[str, str], List[str]]:
        named: Dict[str, str] = {}
        positional: List[str] = []

        if not additional_arguments or additional_arguments.strip() == "":
            return named, positional

        parts = self.split_arguments(additional_arguments)
        for part in parts:
            trimmed = part.strip()
            if trimmed.startswith("/"):
                match = re.match(r"/([^:=]+)([:=](.+))?", trimmed)
                if match:
                    name = match.group(1).strip()
                    value = match.group(3).strip() if match.group(3) else ""
                    named[name] = value
                    logger.debug(f"Parsed named argument: {name} = {value}")
                    continue
            positional.append(trimmed)
            logger.debug(f"Parsed positional argument: {trimmed}")
        return named, positional

    def _parse_action_arguments(
        self, additional_arguments: str
    ) -> Tuple[Dict[str, str], List[str]]:
        """
        Parse arguments with Unix-style flags (-l, --limit) and positional arguments.

        Args:
            additional_arguments: The argument string to parse

        Returns:
            Tuple of (named_args, positional_args)
        """
        named: Dict[str, str] = {}
        positional: List[str] = []

        if not additional_arguments or additional_arguments.strip() == "":
            return named, positional

        parts = self.split_arguments(additional_arguments)
        i = 0
        while i < len(parts):
            part = parts[i].strip()

            # Check for --long-flag=value format
            if part.startswith("--") and "=" in part:
                flag_part = part[2:]  # Remove --
                flag_name, flag_value = flag_part.split("=", 1)
                named[flag_name] = flag_value
                logger.debug(f"Parsed named argument: {flag_name} = {flag_value}")
                i += 1
                continue

            # Check for --long-flag format (value in next part)
            if part.startswith("--"):
                flag_name = part[2:]
                if i + 1 < len(parts) and not parts[i + 1].startswith("-"):
                    flag_value = parts[i + 1]
                    named[flag_name] = flag_value
                    logger.debug(f"Parsed named argument: {flag_name} = {flag_value}")
                    i += 2
                else:
                    named[flag_name] = ""
                    logger.debug(f"Parsed named argument (no value): {flag_name}")
                    i += 1
                continue

            # Check for -f format (short flag, value in next part)
            if part.startswith("-") and len(part) == 2:
                flag_name = part[1]
                if i + 1 < len(parts) and not parts[i + 1].startswith("-"):
                    flag_value = parts[i + 1]
                    named[flag_name] = flag_value
                    logger.debug(f"Parsed named argument: {flag_name} = {flag_value}")
                    i += 2
                else:
                    named[flag_name] = ""
                    logger.debug(f"Parsed named argument (no value): {flag_name}")
                    i += 1
                continue

            # Otherwise it's a positional argument
            positional.append(part)
            logger.debug(f"Parsed positional argument: {part}")
            i += 1

        return named, positional

    def get_named_argument(
        self, named_args: Dict[str, str], name: str, default: Optional[str] = None
    ) -> Optional[str]:
        return named_args.get(name, default)

    def get_positional_argument(
        self, positional_args: List[str], index: int, default: Optional[str] = None
    ) -> Optional[str]:
        if 0 <= index < len(positional_args):
            return positional_args[index]
        return default

    def get_name(self) -> str:
        return self.__class__.__name__

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return []

    def get_help(self) -> str:
        """
        Get detailed help text for this action.

        Returns:
            Detailed help text explaining what the action does and how to use it.
        """
        # Default implementation using docstring
        return self.__doc__.strip() if self.__doc__ else "No help available."
