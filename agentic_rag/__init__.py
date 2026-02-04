"""Agentic RAG - A Python package for agentic retrieval-augmented generation."""

from .components import ComponentRegistry, get_default_registry
from .config import Config, get_config, get_global_config, set_global_config
from .export import JSONLDExporter
from .pipeline import PipelineFactory, PipelineRunner
from .types import (
    DOCUMENT_STORE,
    ComponentSpec,
    ComponentType,
    DataType,
    PipelineSpec,
    list_available_components,
)

__version__: str = "0.1.0"
__author__: str = "Vardhan Shorewala"
__email__: str = "vardhanshorewala@berkeley.edu"

__all__ = [
    "__version__",
    "__author__",
    "__email__",
    "Config",
    "get_config",
    "get_global_config",
    "set_global_config",
    "PipelineFactory",
    "PipelineRunner",
    "ComponentRegistry",
    "get_default_registry",
    "ComponentSpec",
    "PipelineSpec",
    "DataType",
    "ComponentType",
    "DOCUMENT_STORE",
    "list_available_components",
    "JSONLDExporter",
]
