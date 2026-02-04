"""Component specification and instance definitions."""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .data_types import ComponentType, DataType, PipelineUsage


@dataclass
class ComponentSpec:
    """Specification for a pipeline component."""

    name: str
    component_type: ComponentType
    haystack_class: str
    input_types: List[DataType]
    output_types: List[DataType]
    pipeline_usage: PipelineUsage = PipelineUsage.BOTH
    default_config: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    parallelizable: bool = True

    # NEW: Store the final merged config right here
    runtime_config: Dict[str, Any] = field(default_factory=dict)

    # Store the full type string (e.g., "EMBEDDER.SENTENCE_TRANSFORMERS_DOC")
    full_type: str = ""

    # FAIR Compliance: What content type this component produces
    # e.g., CONVERTER produces DOCUMENT, CHUNKER produces CHUNK, EMBEDDER produces EMBEDDING
    produces_content_type: Optional[str] = None  # String value of ContentType enum

    def is_compatible_input(self, data_type: DataType) -> bool:
        """Check if a data type is compatible with this component's inputs."""
        return data_type in self.input_types

    def configure(self, user_config: Dict[str, Any]) -> "ComponentSpec":
        """Merge user config with defaults and store as runtime config."""
        self.runtime_config = {**self.default_config, **user_config}
        return self

    def get_config(self) -> Dict[str, Any]:
        """Get the runtime config, falling back to default if not configured."""
        return self.runtime_config if self.runtime_config else self.default_config


def _create_chroma_document_store(
    root_dir: str = ".",
    host: Optional[str] = None,
    port: Optional[int] = None,
    collection_name: Optional[str] = None,
) -> Any:
    """Create a ChromaDocumentStore with local or remote persistence.

    Args:
        root_dir: Directory for local persistence (used if host/port not provided)
        host: Optional ChromaDB server host for async support (e.g., "localhost")
        port: Optional ChromaDB server port for async support (e.g., 8000)
        collection_name: Optional collection name for isolation (important for remote)

    Returns:
        ChromaDocumentStore configured for local or remote connection
    """
    try:
        from haystack_integrations.document_stores.chroma import ChromaDocumentStore

        # Use remote connection if host/port provided (enables async)
        if host and port:
            # Remote connection - supports async operations
            # Collection name is critical for isolation when using same server
            return ChromaDocumentStore(
                host=host,
                port=port,
                collection_name=collection_name or "default_collection",
            )
        else:
            # Local persistence - sync only
            chroma_path = os.path.join(root_dir, "data", "chroma")
            os.makedirs(chroma_path, exist_ok=True)
            return ChromaDocumentStore(persist_path=chroma_path)
    except ImportError as e:
        raise ImportError(
            f"ChromaDocumentStore not available. Install with: pip install chroma-haystack. Error: {e}"
        )


def _create_qdrant_document_store(
    root_dir: str = ".",
    host: Optional[str] = None,
    port: Optional[int] = None,
    collection_name: Optional[str] = None,
    embedding_dim: int = 768,
) -> Any:
    """Create a QdrantDocumentStore with local or remote persistence.

    Qdrant supports async operations with BOTH local and remote storage!

    Args:
        root_dir: Directory for local persistence (used if host/port not provided)
        host: Optional Qdrant server host (e.g., "localhost")
        port: Optional Qdrant server port (e.g., 6333)
        collection_name: Collection name for isolation
        embedding_dim: Dimension of embeddings (default: 768 for all-mpnet-base-v2)

    Returns:
        QdrantDocumentStore configured for local or remote connection
    """
    try:
        from haystack_integrations.document_stores.qdrant import QdrantDocumentStore

        # Use remote connection if host/port provided
        if host and port:
            return QdrantDocumentStore(
                host=host,
                port=port,
                index=collection_name or "default_collection",
                embedding_dim=embedding_dim,
                recreate_index=False,  # Don't drop existing data
                return_embedding=False,
                wait_result_from_api=True,
            )
        else:
            # Local persistence - FULLY ASYNC COMPATIBLE!
            qdrant_path = os.path.join(root_dir, "data", "qdrant")
            os.makedirs(qdrant_path, exist_ok=True)
            return QdrantDocumentStore(
                path=qdrant_path,
                index=collection_name or "default_collection",
                embedding_dim=embedding_dim,
                recreate_index=False,
                return_embedding=False,
                wait_result_from_api=True,
            )
    except ImportError as e:
        raise ImportError(
            f"QdrantDocumentStore not available. Install with: pip install qdrant-haystack. Error: {e}"
        )


def create_haystack_component(spec: ComponentSpec) -> Any:
    """Create a Haystack component from specification - handles config internally."""
    # Get the final config from ComponentSpec
    config = spec.get_config().copy()  # Copy to avoid modifying original

    # Check if this is a Chroma component by name
    is_chroma_component = "chroma" in spec.name.lower()

    # Check if this is a Qdrant component by name
    is_qdrant_component = "qdrant" in spec.name.lower()

    # Check config for Qdrant-specific params (for remote Qdrant)
    has_qdrant_config = any(
        k in config
        for k in ["qdrant_host", "qdrant_port", "qdrant_collection", "embedding_dim"]
    )

    # Special handling for Chroma components - inject document store
    if "ChromaEmbeddingRetriever" in spec.haystack_class or (
        "DocumentWriter" in spec.haystack_class and is_chroma_component
    ):
        # Get root directory from config or use current directory
        config.pop("model", None)  # Remove model if present for these components
        root_dir = config.pop("root_dir", ".")

        # Check for remote ChromaDB configuration (for async support)
        chroma_host = config.pop("chroma_host", None)
        chroma_port = config.pop("chroma_port", None)

        # Fallback to env vars if not in config (Safety net)
        if not chroma_host:
            chroma_host = os.getenv("CHROMA_HOST")
        if not chroma_port:
            chroma_port = os.getenv("CHROMA_PORT")

        if chroma_port:
            chroma_port = int(chroma_port)

        chroma_collection = config.pop("chroma_collection", None)

        # DEBUG LOG
        print(f"DEBUG: Creating Chroma component {spec.name}")
        print(
            f"DEBUG: chroma_host={chroma_host}, chroma_port={chroma_port}, collection={chroma_collection}"
        )

        document_store = _create_chroma_document_store(
            root_dir=root_dir,
            host=chroma_host,
            port=chroma_port,
            collection_name=chroma_collection,
        )
        config["document_store"] = document_store

    # Special handling for Qdrant components - inject document store
    elif "QdrantEmbeddingRetriever" in spec.haystack_class or (
        "DocumentWriter" in spec.haystack_class
        and (is_qdrant_component or has_qdrant_config)
    ):
        # Get root directory from config or use current directory
        config.pop("model", None)  # Remove model if present for these components
        root_dir = config.pop("root_dir", ".")

        # Check for remote Qdrant configuration
        qdrant_host = config.pop("qdrant_host", None)
        qdrant_port = config.pop("qdrant_port", None)
        qdrant_collection = config.pop("qdrant_collection", None)
        embedding_dim = config.pop("embedding_dim", 768)  # Default to 768

        document_store = _create_qdrant_document_store(
            root_dir=root_dir,
            host=qdrant_host,
            port=qdrant_port,
            collection_name=qdrant_collection,
            embedding_dim=embedding_dim,
        )
        config["document_store"] = document_store

    elif "DocumentWriter" in spec.haystack_class:
        # DocumentWriter without specific store config - remove root_dir if present
        config.pop("root_dir", None)

    # Dynamic import and instantiation
    module_path, class_name = spec.haystack_class.rsplit(".", 1)

    try:
        import importlib

        module = importlib.import_module(module_path)
        component_class = getattr(module, class_name)
        return component_class(**config)
    except (ImportError, AttributeError) as e:
        raise ImportError(
            f"Failed to import Haystack component {spec.haystack_class}: {e}"
        )
