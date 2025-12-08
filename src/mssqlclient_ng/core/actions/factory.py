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

    @classmethod
    def register(cls, name: str, description: str):
        """
        Decorator to register an action class with the factory.

        Usage:
            @ActionFactory.register("whoami", "Display current user context")
            class Whoami(BaseAction):
                ...

        Args:
            name: The action name (command)
            description: Human-readable description of the action
        """

        def decorator(action_class: Type[BaseAction]):
            cls._registry[name.lower()] = (action_class, description)
            return action_class

        return decorator

    @classmethod
    def register_action(
        cls, name: str, action_class: Type[BaseAction], description: str
    ) -> None:
        """
        Manually register an action class.

        Args:
            name: The action name (command)
            action_class: The action class
            description: Human-readable description of the action
        """
        cls._registry[name.lower()] = (action_class, description)

    @classmethod
    def get_action(cls, action_type: str) -> BaseAction | None:
        """_summary_

        Args:
            action_type (str): _description_

        Returns:
            BaseAction | None: _description_
        """
        action_key = action_type.lower()

        if action_key not in cls._registry:
            return None

        action_class, _ = cls._registry[action_key]
        return action_class()

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
                # Assuming actions have a get_arguments method
                arguments = (
                    action.get_arguments() if hasattr(action, "get_arguments") else []
                )
                result.append((name, description, arguments))
            except Exception as e:
                logger.warning(f"Could not instantiate action '{name}': {e}")
                result.append((name, description, []))

        return result

    @classmethod
    def get_action_type(cls, action_name: str) -> Optional[Type[BaseAction]]:
        """
        Get the class type of an action by its name.

        Args:
            action_name: The action name

        Returns:
            The action class type, or None if not found
        """
        action_key = action_name.lower()
        if action_key in cls._registry:
            return cls._registry[action_key][0]
        return None

    @classmethod
    def get_action_description(cls, action_name: str) -> Optional[str]:
        """
        Get the description of an action by its name.

        Args:
            action_name: The action name

        Returns:
            The action description, or None if not found
        """
        action_key = action_name.lower()
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
        Check if an action is registered.

        Args:
            action_name: The action name

        Returns:
            True if action exists, False otherwise
        """
        return action_name.lower() in cls._registry

    @classmethod
    def clear_registry(cls) -> None:
        """Clear all registered actions. Mainly for testing."""
        cls._registry.clear()
