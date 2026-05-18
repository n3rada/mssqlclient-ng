# mssqlclient_ng/core/actions/factory.py

# Built-in imports
from typing import Dict, Type, List, Tuple, Optional

# Third party imports
from loguru import logger

# Local library imports
from .base import BaseAction


class ActionFactory:
    """
    Factory for creating and managing action instances.
    Uses a registry pattern to map action names to their classes and descriptions.
    """

    # Action registry: maps action names to (class, description)
    _registry: Dict[str, Tuple[Type[BaseAction], str]] = {}

    # Alias registry: maps alias -> canonical action name
    _aliases: Dict[str, str] = {}

    @classmethod
    def register(cls, name: str, description: str, aliases: Optional[List[str]] = None):
        """
        Decorator to register an action class with the factory.

        Usage:
            @ActionFactory.register("whoami", "Display current user context", aliases=["id"])
            class Whoami(BaseAction):


        Args:
            name: The action name (command)
            description: Human-readable description of the action
            aliases: Optional list of alternative names for this action
        """

        def decorator(action_class: Type[BaseAction]):
            cls._registry[name.lower()] = (action_class, description)
            if aliases:
                for alias in aliases:
                    cls._aliases[alias.lower()] = name.lower()
            return action_class

        return decorator

    @classmethod
    def get_action(cls, action_type: str) -> BaseAction | None:
        """Get an action instance by name or alias.

        Args:
            action_type: The action name or alias

        Returns:
            An instance of the action, or None if not found
        """
        action_key = action_type.lower()

        # Resolve alias
        action_key = cls._aliases.get(action_key, action_key)

        if action_key not in cls._registry:
            return None

        action_class, _ = cls._registry[action_key]
        return action_class()

    @classmethod
    def resolve_alias(cls, action_type: str) -> str:
        """Resolve an action name or alias to its canonical name."""
        return cls._aliases.get(action_type.lower(), action_type.lower())

    @classmethod
    def get_available_actions(cls) -> List[Tuple[str, str, List[str]]]:
        """
        Get a list of all available actions with their descriptions and arguments.

        Returns:
            List of tuples: (action_name, description, arguments)
        """
        result = []

        for name, (action_class, description) in cls._registry.items():
            try:
                action = action_class()
                getter = getattr(action, "get_arguments", None)
                arguments = getter() if getter is not None else []
                result.append((name, description, arguments))
            except Exception as e:
                logger.warning(f"Could not instantiate action '{name}': {e}")
                result.append((name, description, []))

        return result

    @classmethod
    def get_action_description(cls, action_name: str) -> Optional[str]:
        """
        Get the description of an action by its name or alias.

        Args:
            action_name: The action name or alias

        Returns:
            The action description, or None if not found
        """
        action_key = cls._aliases.get(action_name.lower(), action_name.lower())
        if action_key in cls._registry:
            return cls._registry[action_key][1]
        return None

    @classmethod
    def list_actions(cls) -> List[str]:
        """
        Get a list of all registered action names.

        Returns:
            List of action names
        """
        return list(cls._registry.keys())

    @classmethod
    def action_exists(cls, action_name: str) -> bool:
        """
        Check if an action is registered (by name or alias).

        Args:
            action_name: The action name or alias

        Returns:
            True if action exists, False otherwise
        """
        key = action_name.lower()
        return key in cls._registry or key in cls._aliases

    @classmethod
    def list_aliases(cls) -> Dict[str, str]:
        """
        Get all registered action aliases.

        Returns:
            Dict mapping alias -> canonical action name
        """
        return dict(cls._aliases)

    @classmethod
    def get_action_category(cls, action_name: str) -> str:
        """
        Derive the category for an action from its module path.

        E.g. mssqlclient_ng.core.actions.database.whoami -> "database"
        Falls back to "other" when the module structure is unexpected.
        """
        key = cls._aliases.get(action_name.lower(), action_name.lower())
        if key not in cls._registry:
            return "other"
        action_class, _ = cls._registry[key]
        parts = action_class.__module__.split(".")
        try:
            idx = parts.index("actions")
            if idx + 1 < len(parts):
                return parts[idx + 1]
        except ValueError:
            pass
        return "other"

    @classmethod
    def clear_registry(cls) -> None:
        """Clear all registered actions. Mainly for testing."""
        cls._registry.clear()

    @classmethod
    def display_action_help(cls, action_name: str) -> None:
        """Display argparse-style help for an action."""
        from .base import Arg

        action = cls.get_action(action_name)
        if action is None:
            print(f"Unknown action: {action_name}")
            return

        description = cls.get_action_description(action_name) or ""
        canonical = cls.resolve_alias(action_name)
        aliases = [a for a, c in cls._aliases.items() if c == canonical]

        # Build usage line from Arg descriptors
        arg_fields = sorted(
            action._get_arg_fields().items(),
            key=lambda kv: (kv[1].position if kv[1].position >= 0 else 999),
        )

        usage_tokens = [f"!{action_name}"]
        positional_args = []
        option_args = []

        for name, arg in arg_fields:
            label = (arg.long_name or arg.short_name or name.lstrip("_")).upper()
            if arg.position >= 0 and not arg.short_name and not arg.long_name:
                # Pure positional
                token = label if arg.required else f"[{label}]"
                if arg.remainder:
                    token = f"[{label} ...]" if not arg.required else f"{label} ..."
                usage_tokens.append(token)
                positional_args.append((label, arg))
            else:
                # Named / flag
                if arg.toggle:
                    flag = (
                        f"-{arg.short_name}" if arg.short_name else f"--{arg.long_name}"
                    )
                    token = f"[{flag}]"
                else:
                    parts = []
                    if arg.short_name:
                        parts.append(f"-{arg.short_name}")
                    if arg.long_name:
                        parts.append(f"--{arg.long_name}")
                    flag = ", ".join(parts) if parts else f"--{name.lstrip('_')}"
                    token = f"[{flag} {label}]"
                usage_tokens.append(token)
                option_args.append((label, flag, arg))

        print()
        print(f"usage: {' '.join(usage_tokens)}")
        if aliases:
            print(f"aliases: {', '.join(aliases)}")
        print()
        print(description)

        # Class docstring (skip if same as description)
        doc = (action.__class__.__doc__ or "").strip()
        if doc and doc != description:
            print()
            for line in doc.splitlines():
                print(f"  {line}" if line.strip() else "")

        # Positional arguments section
        if positional_args:
            print()
            print("positional arguments:")
            col = max(len(label) for label, _ in positional_args) + 4
            for label, arg in positional_args:
                desc = arg.description or ""
                if arg.default is not None and arg.default != "":
                    desc += f" (default: {arg.default})"
                print(f"  {label:<{col}}{desc}")

        # Options section
        if option_args:
            print()
            print("options:")
            col = max(len(f"{flag} {label}") for label, flag, _ in option_args) + 4
            for label, flag, arg in option_args:
                if arg.toggle:
                    entry = flag
                else:
                    entry = f"{flag} {label}"
                desc = arg.description or ""
                if arg.required:
                    desc += " (required)"
                elif arg.default is not None and arg.default != "" and not arg.toggle:
                    desc += f" (default: {arg.default})"
                print(f"  {entry:<{col}}{desc}")

        print()
