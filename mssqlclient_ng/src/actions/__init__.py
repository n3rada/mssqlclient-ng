"""
Actions package.
Import all action subpackages to ensure actions are registered with the factory.
"""
from . import execution

__all__ = ["execution"]
