"""InGate component for cache lookup and input filtering.

The InGate checks if input data has already been processed by a component
with a specific configuration. It splits inputs into cached vs uncached items.
"""

import hashlib
import json
from typing import Any, Dict, List, Optional

from ...utils.akave_client import AkaveClient
from ...utils.logger import get_logger
from ..neo4j_manager import GraphStore


class InGate:
    """
    InGate handles cache lookups before component processing.

    Responsibilities:
    - Fingerprint input data
    - Query Neo4j for cached results
    - Split data into cached (bypass component) and uncached (process)
    - Track cache hit/miss statistics

    Flow:
        Input Data → [Fingerprint] → [Neo4j Lookup] → Split:
                                                        ├─> Cached (from graph)
                                                        └─> Uncached (to component)
    """

    def __init__(
        self,
        graph_store: GraphStore,
        component_id: str,
        component_name: str,
        username: Optional[str] = None,
        storage_client: Optional[AkaveClient] = None,
        retrieve_from_storage: bool = False,
    ):
        """
        Initialize InGate.

        Args:
            graph_store: Neo4j graph store for cache lookups
            component_id: Unique ID of the component this gate protects
            component_name: Human-readable component name
            username: Username for per-user logging
            storage_client: Optional Akave storage client (creates one if not provided)
            retrieve_from_storage: If True, retrieves actual data from storage (default: False, returns metadata only)
        """
        self.graph_store = graph_store
        self.component_id = component_id
        self.component_name = component_name
        self.storage_client = storage_client or AkaveClient()
        self.retrieve_from_storage = retrieve_from_storage
        self.logger = get_logger(f"{__name__}.{component_name}", username=username)

    def check_cache_batch(
        self, input_items: List[Any], component_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Check cache for multiple items and split into cached vs uncached.

        Handles both single items and batches - just pass a list.

        Args:
            input_items: List of data items to check (or single item in a list)
            component_config: Component configuration dict

        Returns:
            Dictionary with:
            {
                "cached": [(item, cached_result), ...],
                "uncached": [item, ...],
                "fingerprints": {item_idx: fingerprint, ...}
            }

            cached_result format:
            - If retrieve_from_storage=False: List[{fingerprint, object_key, data_type}]
            - If retrieve_from_storage=True: List of actual data retrieved from storage

        Example (batch):
            >>> result = ingate.check_cache_batch(
            ...     input_items=[doc1, doc2, doc3],
            ...     component_config={"model": "minilm"}
            ... )
            >>> # Process only uncached
            >>> if result['uncached']:
            ...     new_results = component.run(result['uncached'])

        Example (single item):
            >>> result = ingate.check_cache_batch(
            ...     input_items=[single_doc],  # Just wrap in list
            ...     component_config={"chunk_size": 500}
            ... )
        """
        # Hash the config once
        config_hash = self.hash_config(component_config)

        # Fingerprint all inputs
        fingerprints = {}
        for idx, item in enumerate(input_items):
            fingerprints[idx] = self.fingerprint_data(item)

        # Batch lookup in Neo4j
        fingerprint_list = list(fingerprints.values())
        cache_map = self.graph_store.lookup_cached_transformations_batch(
            input_fingerprints=fingerprint_list,
            component_id=self.component_id,
            config_hash=config_hash,
        )

        # DEBUG: Log cache lookup results
        self.logger.info(
            f"Cache lookup: queried {len(fingerprint_list)} fingerprints, "
            f"found {len(cache_map)} cached results (component_id={self.component_id})"
        )

        # Split into cached vs uncached
        cached = []
        uncached = []

        cache_hits = 0
        for idx, item in enumerate(input_items):
            fp = fingerprints[idx]
            if fp in cache_map:
                # Found cached result
                cached_metadata = cache_map[fp]

                if self.retrieve_from_storage:
                    try:
                        cached_data = []
                        for output_meta in cached_metadata:
                            object_key = output_meta[
                                "ipfs_hash"
                            ]  # Field name kept for Neo4j compat
                            data_type = output_meta.get("data_type")
                            data = self._retrieve_from_storage(object_key, data_type)
                            cached_data.append(data)
                        cached.append((item, cached_data))
                        cache_hits += 1
                    except (ConnectionError, Exception) as e:
                        # Storage retrieval failed - treat as cache miss
                        self.logger.warning(f"Storage retrieval failed for {fp}: {e}")
                        uncached.append(item)
                else:
                    # Just return metadata (fingerprints + object keys)
                    cached.append((item, cached_metadata))
                    cache_hits += 1
            else:
                # No cache, needs processing
                uncached.append(item)

        self.logger.info(
            f"Cache check: {cache_hits} hits, {len(uncached)} misses (total: {len(input_items)})"
        )

        return {
            "cached": cached,
            "uncached": uncached,
            "fingerprints": fingerprints,
        }

    async def check_cache_batch_async(
        self, input_items: List[Any], component_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Async version of check_cache_batch.

        Check cache for multiple items and split into cached vs uncached.

        Args:
            input_items: List of data items to check
            component_config: Component configuration dict

        Returns:
            Dictionary with:
            {
                "cached": [(item, cached_result), ...],
                "uncached": [item, ...],
                "fingerprints": {item_idx: fingerprint, ...}
            }
        """
        # Hash the config once
        config_hash = self.hash_config(component_config)

        # Fingerprint all inputs
        fingerprints = {}
        for idx, item in enumerate(input_items):
            fingerprints[idx] = self.fingerprint_data(item)

        # Batch lookup in Neo4j (async)
        fingerprint_list = list(fingerprints.values())
        cache_map = await self.graph_store.lookup_cached_transformations_batch_async(
            input_fingerprints=fingerprint_list,
            component_id=self.component_id,
            config_hash=config_hash,
        )

        # DEBUG: Log cache lookup results
        self.logger.info(
            f"Cache lookup (async): queried {len(fingerprint_list)} fingerprints, "
            f"found {len(cache_map)} cached results (component_id={self.component_id})"
        )

        # Split into cached vs uncached
        cached = []
        uncached = []

        cache_hits = 0
        for idx, item in enumerate(input_items):
            fp = fingerprints[idx]
            if fp in cache_map:
                # Found cached result
                cached_metadata = cache_map[fp]

                if self.retrieve_from_storage:
                    try:
                        cached_data = []
                        for output_meta in cached_metadata:
                            object_key = output_meta[
                                "ipfs_hash"
                            ]  # Field name kept for Neo4j compat
                            data_type = output_meta.get("data_type")
                            data = await self._retrieve_from_storage_async(
                                object_key, data_type
                            )
                            cached_data.append(data)
                        cached.append((item, cached_data))
                        cache_hits += 1
                    except (ConnectionError, Exception) as e:
                        # Storage retrieval failed - treat as cache miss
                        self.logger.warning(f"Storage retrieval failed for {fp}: {e}")
                        uncached.append(item)
                else:
                    # Just return metadata (fingerprints + object keys)
                    cached.append((item, cached_metadata))
                    cache_hits += 1
            else:
                # No cache, needs processing
                uncached.append(item)

        self.logger.info(
            f"Cache check (async): {cache_hits} hits, {len(uncached)} misses (total: {len(input_items)})"
        )

        return {
            "cached": cached,
            "uncached": uncached,
            "fingerprints": fingerprints,
        }

    def fingerprint_data(self, data: Any) -> str:
        """
        Create stable fingerprint hash for data.

        Args:
            data: Data to fingerprint (Document, ByteStream, str, List, etc.)

        Returns:
            SHA256 hash string (e.g., "fp_abc123...")

        Strategy:
            - Document: hash(content + metadata + id)
            - List[Document]: hash of individual hashes
            - ByteStream: hash(file_content + filename)
            - str: hash(content)
            - List[float]: hash(vector values)
        """
        # Serialize data to string for hashing
        data_str = self._serialize_for_fingerprint(data)

        # Create SHA256 hash
        hash_obj = hashlib.sha256(data_str.encode("utf-8"))
        return f"fp_{hash_obj.hexdigest()[:16]}"

    def hash_config(self, config: Dict[str, Any]) -> str:
        """
        Create stable hash of component configuration.

        Args:
            config: Component configuration dictionary

        Returns:
            Hash string (e.g., "cfg_abc123...")

        Notes:
            - Sorts keys for stability
            - Only hashes "semantic" config (affects output)
            - Excludes performance-only settings (batch_size, num_workers, etc.)
        """
        # Performance-only settings that don't affect output
        PERF_ONLY_KEYS = {
            "batch_size",
            "num_workers",
            "device",
            "show_progress",
            "verbose",
            "debug",
            "workers",
            "threads",
        }

        # Filter to semantic config only
        semantic_config = {k: v for k, v in config.items() if k not in PERF_ONLY_KEYS}

        # Sort keys for stability and create JSON
        config_str = json.dumps(semantic_config, sort_keys=True, separators=(",", ":"))

        # Hash and return short version
        hash_obj = hashlib.sha256(config_str.encode("utf-8"))
        return f"cfg_{hash_obj.hexdigest()[:16]}"

    def _retrieve_from_storage(
        self, object_key: str, data_type: Optional[str] = None
    ) -> Any:
        """Retrieve from Akave storage and reconstruct as Document."""
        from haystack import Document

        # Try to retrieve as JSON first (may contain embedding)
        try:
            data = self.storage_client.retrieve_json(object_key)
            if isinstance(data, dict) and "content" in data:
                # Reconstruct Document with all fields
                return Document(
                    content=data.get("content", ""),
                    meta=data.get("meta", {}),
                    id=data.get("id"),
                    embedding=data.get("embedding"),  # May be None or list
                )
        except Exception:
            pass

        # Fallback: retrieve as text
        text = self.storage_client.retrieve_text(object_key)
        return Document(content=text)

    async def _retrieve_from_storage_async(
        self, object_key: str, data_type: Optional[str] = None
    ) -> Any:
        """Async version of _retrieve_from_storage."""
        from haystack import Document

        # Try to retrieve as JSON first (may contain embedding)
        try:
            data = await self.storage_client.retrieve_json_async(object_key)
            if isinstance(data, dict) and "content" in data:
                # Reconstruct Document with all fields
                return Document(
                    content=data.get("content", ""),
                    meta=data.get("meta", {}),
                    id=data.get("id"),
                    embedding=data.get("embedding"),  # May be None or list
                )
        except Exception:
            pass

        # Fallback: retrieve as text
        text = await self.storage_client.retrieve_text_async(object_key)
        return Document(content=text)

    def _serialize_for_fingerprint(self, data: Any) -> str:
        """
        Serialize data to string for hashing.

        Args:
            data: Data to serialize

        Returns:
            String representation for hashing
        """
        # Check for embedding first (embedder output)
        if hasattr(data, "embedding") and data.embedding is not None:
            # Hash the embedding vector (the new data added by embedder)
            import numpy as np

            if isinstance(data.embedding, np.ndarray):
                return hashlib.sha256(data.embedding.tobytes()).hexdigest()
            else:
                # List of floats
                return str(data.embedding)

        # Handle content attribute (Document, etc.)
        if hasattr(data, "content"):
            return str(data.content) if data.content else ""

        # Handle data attribute (ByteStream)
        if hasattr(data, "data"):
            if isinstance(data.data, bytes):
                return hashlib.sha256(data.data).hexdigest()
            return str(data.data)

        # Handle lists
        if isinstance(data, list):
            return "|".join(str(item) for item in data)

        # Handle dicts
        if isinstance(data, dict):
            return json.dumps(data, sort_keys=True)

        # Basic types: just string it
        return str(data)
