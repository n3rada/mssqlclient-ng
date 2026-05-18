# mssqlclient_ng/core/actions/base.py

# Built-in imports
import shlex
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional, Any, Union, overload

# Third party imports
from loguru import logger


class Arg:
    """
    Argument descriptor for declarative argument binding.

    Declare fields as class variables:
        _dll_path = Arg(position=0, required=True, description="Path to DLL")
        _limit = Arg(short_name="l", long_name="limit", default=25)
        _args = Arg(position=3, remainder=True, description="Function args")
        _command = Arg(position=0, remainder=True, required=True, description="Command")

    The base class _bind_arguments() introspects these and auto-binds values.
    """

    def __init__(
        self,
        position: int = -1,
        short_name: Optional[str] = None,
        long_name: Optional[str] = None,
        required: bool = False,
        remainder: bool = False,
        toggle: bool = False,
        description: Optional[str] = None,
        default: Any = None,
    ):
        self.position = position
        self.short_name = short_name
        self.long_name = long_name
        self.required = required
        self.remainder = remainder
        self.toggle = toggle
        self.description = description
        self.default = default

    def __set_name__(self, owner: type, name: str) -> None:
        self._attr_name = name

    @overload
    def __get__(self, obj: None, objtype: type) -> "Arg": ...
    @overload
    def __get__(self, obj: object, objtype: type) -> Any: ...
    def __get__(self, obj: Optional[object], objtype: Optional[type] = None) -> Any:
        if obj is None:
            return self
        try:
            return obj.__dict__[self._attr_name]
        except KeyError:
            return self.default

    def __set__(self, obj: object, value: Any) -> None:
        obj.__dict__[self._attr_name] = value


# Type alias for the value returned by execute().
# list[dict]             — single tabular result; re-rendered on every cache hit
#                          using the active OutputFormatter.
# list[list[dict]]       — multiple independent tables (e.g. roles prints four);
#                          each sub-list is re-rendered in order on cache hit.
# None                   — pure side-effect action (mutations, file ops, etc.)
#                          or actions that manage their own unstructured output.
ActionResult = Optional[Union[List[Dict[str, Any]], List[List[Dict[str, Any]]]]]


class BaseAction(ABC):
    """
    Abstract base class for all actions, enforcing validation and execution logic.

    ARGUMENT BINDING:
    =================

    DEFAULT BEHAVIOR (auto-binding):
    --------------------------------
    Declare fields with Arg() descriptors and let the base class handle binding:

        _table_name = Arg(position=0, required=True, description="Table name")
        _limit = Arg(position=1, short_name="l", long_name="limit", default=25)

    If no custom validation is needed, don't override validate_arguments().
    The base class will call _bind_arguments() automatically.

    MANUAL PARSING (for complex cases):
    ------------------------------------
    Override validate_arguments() and use _parse_action_arguments(),
    get_named_argument(), get_positional_argument() for custom logic.
    """

    # After this many positional args, treat everything else as positional
    # regardless of flag-like syntax (like C# Remainder = true).
    # Auto-computed from Arg(remainder=True) fields, or set manually.
    _remainder_after: Optional[int] = None

    def validate_arguments(self, additional_arguments: str = "") -> None:
        """
        Validate and bind action arguments.

        Default implementation calls _bind_arguments() for automatic field binding
        using Arg() descriptors. Override for custom validation logic.
        """
        self._bind_arguments(additional_arguments)

    @abstractmethod
    def execute(self, database_context: Any) -> ActionResult:
        """
        Execute the action against the given database context.

        Return value contract (drives format-agnostic caching):
          list[dict]          single tabular result — print it, then return it
          list[list[dict]]    multiple independent tables — print each, then return the list
          None                side-effect-only or unstructured output; no JSON caching
        """
        ...

    @classmethod
    def _get_arg_fields(cls) -> Dict[str, "Arg"]:
        """Get all Arg() descriptors from the class hierarchy."""
        fields = {}
        for klass in reversed(cls.__mro__):
            for name, value in vars(klass).items():
                if isinstance(value, Arg):
                    fields[name] = value
        return fields

    @classmethod
    def _compute_remainder_after(cls) -> Optional[int]:
        """Compute _remainder_after from Arg(remainder=True) fields."""
        for _, arg in cls._get_arg_fields().items():
            if arg.remainder and arg.position >= 0:
                return arg.position
        return None

    def _bind_arguments(self, additional_arguments: str = "") -> None:
        """
        Automatically bind parsed arguments to Arg() fields (like C# BindArguments).

        Introspects Arg descriptors, parses the arguments, then assigns values.
        """
        arg_fields = self._get_arg_fields()

        if not arg_fields:
            # No Arg descriptors, nothing to bind
            return

        # Compute remainder_after from metadata if not set manually
        if self._remainder_after is None:
            self._remainder_after = self._compute_remainder_after()

        # Initialize defaults
        for name, arg in arg_fields.items():
            if arg.default is not None:
                setattr(self, name, arg.default)
            else:
                setattr(self, name, "" if arg.position >= 0 else None)

        if not additional_arguments or not additional_arguments.strip():
            # Check required fields
            for name, arg in arg_fields.items():
                if arg.required:
                    field_label = arg.long_name or arg.short_name or name.lstrip("_")
                    raise ValueError(f"Required argument '{field_label}' is missing.")
            return

        named_args, positional_args = self._parse_action_arguments(additional_arguments)

        # Bind each field
        for name, arg in arg_fields.items():
            value = None

            # Try named arguments first (short name, then long name)
            if arg.short_name:
                value = named_args.get(arg.short_name)
            if value is None and arg.long_name:
                value = named_args.get(arg.long_name)

            # Fall back to positional argument
            if value is None and arg.position >= 0:
                if arg.remainder and arg.position < len(positional_args):
                    value = " ".join(positional_args[arg.position :])
                elif arg.position < len(positional_args):
                    value = positional_args[arg.position]

            # Convert and assign
            if value is not None:
                if arg.toggle:
                    setattr(self, name, self._parse_toggle(value, name))
                else:
                    setattr(self, name, value)
            elif arg.required:
                field_label = arg.long_name or arg.short_name or name.lstrip("_")
                raise ValueError(f"Required argument '{field_label}' is missing.")

    @staticmethod
    def _parse_toggle(value: str, field_name: str) -> bool:
        """Parse toggle values (+/-, on/off, 1/0, true/false, enable/disable)."""
        aliases = {
            "+": True,
            "add": True,
            "on": True,
            "1": True,
            "true": True,
            "enable": True,
            "-": False,
            "del": False,
            "off": False,
            "0": False,
            "false": False,
            "disable": False,
        }
        lower = value.lower().strip()
        if lower in aliases:
            return aliases[lower]
        # Bare flag (no value) means True
        if not value:
            return True
        raise ValueError(
            f"Invalid toggle value for '{field_name}': '{value}'. "
            f"Use: on/off, +/-, 1/0, true/false, enable/disable"
        )

    def split_arguments(
        self, additional_arguments: str, separator: str = ","
    ) -> List[str]:
        if not additional_arguments or additional_arguments.strip() == "":
            logger.debug("No arguments provided.")
            return []

        # Use shlex to split respecting quotes
        # Use posix=True (default) to properly strip quotes from arguments
        try:
            splitted = [
                arg for arg in shlex.split(additional_arguments) if arg != separator
            ]
            logger.debug(f"Splitted arguments: {splitted}")
            return splitted
        except ValueError as e:
            # If shlex fails (e.g., unclosed quotes), fall back to simple split
            logger.warning(f"shlex parsing failed: {e}. Falling back to simple split.")
            return [
                arg.strip()
                for arg in additional_arguments.split()
                if arg.strip() and arg != separator
            ]

    def _parse_action_arguments(
        self,
        additional_arguments: str = "",
        argument_list: Optional[List[str]] = None,
    ) -> Tuple[Dict[str, str], List[str]]:
        """
        Parse arguments with Unix-style flags (-l, --limit) and positional arguments.

        After _remainder_after positional args are collected, all remaining parts
        are treated as positional regardless of flag-like syntax (like C# Remainder = true).

        Args:
            additional_arguments: The argument string to parse (deprecated, use argument_list)
            argument_list: Pre-split list of arguments (preferred)

        Returns:
            Tuple of (named_args, positional_args)
        """
        named: Dict[str, str] = {}
        positional: List[str] = []

        # Prefer pre-split list to avoid double-parsing
        if argument_list is not None:
            parts = argument_list
        elif not additional_arguments or additional_arguments.strip() == "":
            return named, positional
        else:
            parts = self.split_arguments(additional_arguments)
        i = 0
        while i < len(parts):
            part = parts[i].strip()

            # Once we've collected enough positional args, treat the rest as positional
            if (
                self._remainder_after is not None
                and len(positional) >= self._remainder_after
            ):
                positional.append(part)
                logger.debug(f"Parsed remainder argument: {part}")
                i += 1
                continue

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
                if (
                    i + 1 < len(parts)
                    and not parts[i + 1].startswith("--")
                    and not (
                        parts[i + 1].startswith("-")
                        and len(parts[i + 1]) == 2
                        and parts[i + 1][1].isalpha()
                    )
                ):
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
            if part.startswith("-") and len(part) == 2 and part[1].isalpha():
                flag_name = part[1]
                if (
                    i + 1 < len(parts)
                    and not parts[i + 1].startswith("--")
                    and not (
                        parts[i + 1].startswith("-")
                        and len(parts[i + 1]) == 2
                        and parts[i + 1][1].isalpha()
                    )
                ):
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
