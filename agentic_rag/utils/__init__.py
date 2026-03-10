"""Utilities for agentic-rag."""

from .akave_client import AkaveClient
from .logger import configure_haystack_logging, get_logger, get_system_logger
from .metrics import MetricsCollector, TimedExecution

# Alias for backward compatibility
StorageClient = AkaveClient

__all__ = [
    "AkaveClient",
    "StorageClient",
    "get_logger",
    "get_system_logger",
    "configure_haystack_logging",
    "MetricsCollector",
    "TimedExecution",
]
