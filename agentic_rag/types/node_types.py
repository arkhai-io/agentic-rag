"""Types for Neo4j nodes."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

# =============================================================================
# FAIR Compliance: URI Namespace and Content Types
# =============================================================================

# Persistent URI namespace for FAIR compliance (F1-F3)
ARKHAI_NAMESPACE = "https://w3id.org/arkhai"


class ContentType(Enum):
    """
    Semantic content type for FAIR compliance.

    Maps to JSON-LD @type during export. Used to distinguish between
    different kinds of data flowing through pipelines.
    """

    DOCUMENT = "Document"  # Original/converted documents
    CHUNK = "Chunk"  # Text chunks from chunking
    EMBEDDING = "Embedding"  # Vector embeddings
    DATA_NODE = "DataNode"  # Generic data (fallback)


def generate_uri(content_type: ContentType, identifier: str) -> str:
    """
    Generate a persistent URI for FAIR compliance (F1-F3).

    Args:
        content_type: The semantic type of the content
        identifier: Unique identifier (e.g., fingerprint)

    Returns:
        URI like https://w3id.org/arkhai/doc/fp_abc123

    Example:
        >>> generate_uri(ContentType.DOCUMENT, "fp_abc123")
        'https://w3id.org/arkhai/doc/fp_abc123'
    """
    type_to_path = {
        ContentType.DOCUMENT: "doc",
        ContentType.CHUNK: "chunk",
        ContentType.EMBEDDING: "embedding",
        ContentType.DATA_NODE: "data",
    }
    path = type_to_path.get(content_type, "data")
    return f"{ARKHAI_NAMESPACE}/{path}/{identifier}"


@dataclass
class ComponentNode:
    """Represents a component node in Neo4j."""

    component_name: str
    pipeline_name: str
    version: str
    author: str
    component_config: Dict[str, Any]
    project: str = "default"  # Project name for multi-tenant isolation
    component_type: Optional[str] = None  # e.g., "EMBEDDER.SENTENCE_TRANSFORMERS_DOC"
    pipeline_type: Optional[str] = None  # "indexing" or "retrieval"
    branch_id: Optional[str] = (
        None  # For retrieval pipelines: identifies which indexing pipeline branch
    )
    id: Optional[str] = None  # Auto-generated if not provided
    cache_key: Optional[str] = None  # Pipeline-agnostic key for cache lookups

    def __post_init__(self) -> None:
        """Generate ID and cache_key if not provided."""
        if self.id is None:
            # Create deterministic hash from: component_name__pipeline_name__project__version__author__branch_id
            import hashlib
            import json

            combined = f"{self.component_name}__{self.pipeline_name}__{self.project}__{self.version}__{self.author}"

            # Include branch_id if provided (for retrieval pipeline branches)
            if self.branch_id:
                combined += f"__{self.branch_id}"

            # Generate SHA-256 hash and take first 12 characters for readability
            hash_obj = hashlib.sha256(combined.encode("utf-8"))
            self.id = f"comp_{hash_obj.hexdigest()[:12]}"

        # Generate pipeline-agnostic cache key (component_type + config + author)
        if self.cache_key is None:
            import hashlib

            # Use component_type + config for cache sharing across pipelines
            config_str = json.dumps(self.component_config, sort_keys=True)
            cache_combined = f"{self.component_type}__{config_str}__{self.author}"

            cache_hash = hashlib.sha256(cache_combined.encode("utf-8"))
            self.cache_key = f"cache_{cache_hash.hexdigest()[:12]}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Neo4j insertion."""
        import json

        # Convert component_config to JSON string for Neo4j compatibility
        config_json = json.dumps(
            self.component_config, sort_keys=True, separators=(",", ":")
        )

        result = {
            "id": self.id,
            "component_name": self.component_name,
            "pipeline_name": self.pipeline_name,
            "project": self.project,
            "version": self.version,
            "author": self.author,
            "component_config_json": config_json,
            "cache_key": self.cache_key,
        }

        if self.component_type:
            result["component_type"] = self.component_type

        if self.pipeline_type:
            result["pipeline_type"] = self.pipeline_type

        if self.branch_id:
            result["branch_id"] = self.branch_id

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ComponentNode":
        """Create ComponentNode from dictionary."""
        import json

        # Always expect JSON format
        component_config = json.loads(data["component_config_json"])

        return cls(
            component_name=data["component_name"],
            pipeline_name=data["pipeline_name"],
            project=data.get("project", "default"),
            version=data["version"],
            author=data["author"],
            component_config=component_config,
            component_type=data.get("component_type"),
            pipeline_type=data.get("pipeline_type"),
            branch_id=data.get("branch_id"),
            id=data.get("id"),
            cache_key=data.get("cache_key"),
        )


@dataclass
class ComponentRelationship:
    """Represents a relationship between component nodes."""

    source_id: str
    target_id: str
    relationship_type: str
    properties: Optional[Dict[str, Any]] = None

    def to_tuple(self) -> Tuple[str, str, str] | Tuple[str, str, str, Dict[str, Any]]:
        """Convert to tuple for batch edge insertion."""
        if self.properties:
            return (
                self.source_id,
                self.target_id,
                self.relationship_type,
                self.properties,
            )
        return (self.source_id, self.target_id, self.relationship_type)


@dataclass
class UserNode:
    """Represents a user who owns pipelines."""

    username: str
    email: Optional[str] = None
    display_name: Optional[str] = None
    id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.id is None:
            base = self.username if not self.email else f"{self.username}__{self.email}"
            self.id = base.replace(" ", "_")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "username": self.username,
            "email": self.email,
            "display_name": self.display_name,
        }


@dataclass
class ProjectNode:
    """Represents a project that contains pipelines."""

    name: str  # Project name (e.g., "my_rag_app")
    username: str  # Owner username
    description: Optional[str] = None
    id: Optional[str] = None
    created_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        """Generate ID from username and project name."""
        if self.id is None:
            import hashlib

            # Create deterministic ID: project_{username}_{name}
            combined = f"{self.username}__{self.name}"
            hash_obj = hashlib.sha256(combined.encode("utf-8"))
            self.id = f"proj_{hash_obj.hexdigest()[:12]}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Neo4j insertion."""
        result = {
            "id": self.id,
            "name": self.name,
            "username": self.username,
        }
        if self.description:
            result["description"] = self.description
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProjectNode":
        """Create ProjectNode from dictionary."""
        return cls(
            name=data["name"],
            username=data["username"],
            description=data.get("description"),
            id=data.get("id"),
        )


# =============================================================================
# FAIR Compliance: Run/Provenance Tracking
# =============================================================================


@dataclass
class RunNode:
    """
    Represents a pipeline execution run for provenance tracking.

    Links all data transformations from a single pipeline execution.
    Required for FAIR compliance (PROV-O: prov:Activity).

    Graph pattern:
        Run -[:GENERATED]-> DataPiece
        DataPiece -[:GENERATED_BY]-> Run

    Example:
        >>> run = RunNode(
        ...     id="run_abc123",
        ...     pipeline_name="indexing_pipeline",
        ...     username="alice",
        ... )
        >>> run.uri
        'https://w3id.org/arkhai/run/run_abc123'
    """

    # Identity (required)
    id: str  # Run identifier (e.g., "run_abc123")
    pipeline_name: str  # Name of the pipeline that was executed
    username: str  # Who initiated the run

    # Pipeline info (optional)
    pipeline_version: Optional[str] = None  # Version of the pipeline
    project: str = "default"  # Project this run belongs to

    # Provenance metadata (optional)
    git_commit: Optional[str] = None  # Git commit hash at run time
    model_names: Optional[List[str]] = None  # Models used in this run
    config_hash: Optional[str] = None  # Hash of pipeline configuration

    # Timing (optional)
    started_at: Optional[datetime] = None  # When run started
    finished_at: Optional[datetime] = None  # When run completed

    # FAIR compliance
    uri: Optional[str] = None  # Persistent URI

    def __post_init__(self) -> None:
        """Generate URI if not provided."""
        if self.uri is None:
            self.uri = f"{ARKHAI_NAMESPACE}/run/{self.id}"

    def to_neo4j_properties(self) -> Dict[str, Any]:
        """Convert to Neo4j node properties."""
        props: Dict[str, Any] = {
            "id": self.id,
            "uri": self.uri,
            "pipeline_name": self.pipeline_name,
            "username": self.username,
            "project": self.project,
        }

        if self.pipeline_version:
            props["pipeline_version"] = self.pipeline_version
        if self.git_commit:
            props["git_commit"] = self.git_commit
        if self.model_names:
            # Neo4j supports lists natively
            props["model_names"] = self.model_names
        if self.config_hash:
            props["config_hash"] = self.config_hash
        if self.started_at:
            props["started_at"] = self.started_at.isoformat()
        if self.finished_at:
            props["finished_at"] = self.finished_at.isoformat()

        return props

    @classmethod
    def from_neo4j_node(cls, node: dict) -> "RunNode":
        """Create RunNode from Neo4j node properties."""
        started_at = None
        if node.get("started_at"):
            started_at = datetime.fromisoformat(node["started_at"])

        finished_at = None
        if node.get("finished_at"):
            finished_at = datetime.fromisoformat(node["finished_at"])

        return cls(
            id=node["id"],
            pipeline_name=node["pipeline_name"],
            username=node.get("username", ""),
            pipeline_version=node.get("pipeline_version"),
            project=node.get("project", "default"),
            git_commit=node.get("git_commit"),
            model_names=node.get("model_names"),
            config_hash=node.get("config_hash"),
            started_at=started_at,
            finished_at=finished_at,
            uri=node.get("uri"),
        )


# =============================================================================
# Data Content Nodes
# =============================================================================


@dataclass
class DataPiece:
    """
    Represents a piece of data in the Neo4j graph.

    Used by InGate/OutGate to track data transformations and enable caching.
    Each unique piece of content gets one DataPiece node (deduplicated by fingerprint).
    Content is stored on Akave (S3-compatible), object key is stored in Neo4j.

    Note: The field `ipfs_hash` is kept for backward compatibility with existing Neo4j
    schemas but now stores Akave object keys instead of IPFS CIDs.

    FAIR Compliance:
        - `content_type`: Semantic type (Document, Chunk, Embedding, DataNode)
        - `generated_by`: Run ID linking to provenance (PROV-O: prov:wasGeneratedBy)
        - `uri`: Persistent URI for identification (F1-F3)

    Example:
        >>> piece = DataPiece(
        ...     fingerprint="fp_abc123",
        ...     ipfs_hash="ak_abc123...",  # Akave object key
        ...     data_type="Document",
        ...     username="alice",
        ...     content_type=ContentType.DOCUMENT,
        ...     generated_by="run_xyz789",
        ... )
        >>> piece.uri
        'https://w3id.org/arkhai/doc/fp_abc123'
    """

    # Identity (required)
    fingerprint: str  # SHA256 hash of content (PRIMARY KEY)

    # Content storage (required)
    ipfs_hash: str  # Akave object key (field name kept for Neo4j schema compatibility)
    data_type: str  # Python type: "Document", "ByteStream", "List[Document]", etc.

    # Authorship (required)
    username: str  # Who created/owns this data

    # FAIR compliance fields (optional, for semantic interoperability)
    content_type: Optional[ContentType] = (
        None  # Semantic type: Document | Chunk | Embedding | DataNode
    )
    generated_by: Optional[str] = None  # Run ID for provenance tracking
    uri: Optional[str] = (
        None  # Persistent URI: https://w3id.org/arkhai/doc/{fingerprint}
    )

    # Metadata (optional)
    content_preview: Optional[str] = None  # First 200 chars for quick viewing
    size_bytes: Optional[int] = None  # Size of content
    created_at: Optional[datetime] = None  # When first created
    source: Optional[str] = None  # Original source (file path, URL, etc.)

    def __post_init__(self) -> None:
        """Generate URI if content_type is set but URI is not."""
        if self.content_type is not None and self.uri is None:
            self.uri = generate_uri(self.content_type, self.fingerprint)

    def to_neo4j_properties(self) -> Dict[str, Any]:
        """Convert to Neo4j node properties."""
        props: Dict[str, Any] = {
            "fingerprint": self.fingerprint,
            "ipfs_hash": self.ipfs_hash,
            "type": self.data_type,  # Keep 'type' for backward compatibility
            "username": self.username,
        }

        # FAIR compliance fields
        if self.content_type is not None:
            props["content_type"] = self.content_type.value
        if self.generated_by:
            props["generated_by"] = self.generated_by
        if self.uri:
            props["uri"] = self.uri

        # Metadata
        if self.content_preview:
            props["content_preview"] = self.content_preview
        if self.size_bytes is not None:
            props["size_bytes"] = self.size_bytes
        if self.source:
            props["source"] = self.source

        return props

    @classmethod
    def from_neo4j_node(cls, node: dict) -> "DataPiece":
        """Create DataPiece from Neo4j node properties."""
        # Parse content_type enum if present
        content_type = None
        if node.get("content_type"):
            try:
                content_type = ContentType(node["content_type"])
            except ValueError:
                pass  # Unknown content type, leave as None

        return cls(
            fingerprint=node["fingerprint"],
            ipfs_hash=node["ipfs_hash"],
            data_type=node["type"],
            username=node["username"],
            content_type=content_type,
            generated_by=node.get("generated_by"),
            uri=node.get("uri"),
            content_preview=node.get("content_preview"),
            size_bytes=node.get("size_bytes"),
            source=node.get("source"),
        )


@dataclass
class TransformedByRelationship:
    """
    Properties for TRANSFORMED_BY relationship.

    Connects input DataPiece to output DataPiece.
    Stores information about the transformation that occurred.
    """

    component_id: str  # Which component did the transformation
    component_name: str  # Human-readable component name
    config_hash: str  # Hash of component configuration

    # Optional metadata
    processing_time_ms: Optional[int] = None
    created_at: Optional[datetime] = None

    def to_neo4j_properties(self) -> Dict[str, Any]:
        """Convert to Neo4j relationship properties."""
        props: Dict[str, Any] = {
            "component_id": self.component_id,
            "component_name": self.component_name,
            "config_hash": self.config_hash,
        }

        if self.processing_time_ms is not None:
            props["processing_time_ms"] = self.processing_time_ms

        return props


@dataclass
class ProcessedByRelationship:
    """
    Properties for PROCESSED_BY relationship.

    Connects DataPiece to Component for tracking/statistics.
    """

    config_hash: str  # Configuration used
    processing_time_ms: Optional[int] = None
    created_at: Optional[datetime] = None

    def to_neo4j_properties(self) -> Dict[str, Any]:
        """Convert to Neo4j relationship properties."""
        props: Dict[str, Any] = {"config_hash": self.config_hash}

        if self.processing_time_ms is not None:
            props["processing_time_ms"] = self.processing_time_ms

        return props
