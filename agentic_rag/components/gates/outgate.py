"""OutGate component for storing results and updating the knowledge graph.

Simple flow:
1. Upload data to IPFS → get IPFS hash
2. Create DataPiece nodes in Neo4j
3. Create TRANSFORMED_BY edges connecting them

FAIR Compliance:
- Supports content_type for semantic typing (Document, Chunk, Embedding)
- Supports run_id for provenance tracking (links to RunNode)
- Generates persistent URIs for each DataPiece
"""

import hashlib
import json
from typing import Any, Dict, List, Optional

from ...types.node_types import ContentType, generate_uri
from ...utils.akave_client import AkaveClient
from ...utils.logger import get_logger
from ..neo4j_manager import GraphStore


class OutGate:
    """
    OutGate stores component outputs to IPFS and Neo4j.

    Simple responsibilities:
    - Upload data to IPFS (placeholder)
    - Create DataPiece nodes with Akave object keys
    - Create TRANSFORMED_BY edges (input → output)

    Flow:
        Output → [Upload to Akave] → [Create DataPiece] → [Create TRANSFORMED_BY edge]
    """

    def __init__(
        self,
        graph_store: GraphStore,
        component_id: str,
        component_name: str,
        username: str,
        storage_client: Optional[AkaveClient] = None,
    ):
        """
        Initialize OutGate.

        Args:
            graph_store: Neo4j graph store
            component_id: Component doing the transformation
            component_name: Human-readable name
            username: Data owner
            storage_client: Optional Akave storage client (creates one if not provided)
        """
        self.graph_store = graph_store
        self.component_id = component_id
        self.component_name = component_name
        self.username = username
        self.storage_client = storage_client or AkaveClient()
        self.logger = get_logger(f"{__name__}.{component_name}", username=username)

    def store(
        self,
        input_data: Any,
        output_data: List[Any],
        component_config: Dict[str, Any],
        processing_time_ms: Optional[int] = None,
        content_type: Optional[ContentType] = None,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Store transformation: input → component → outputs.

        Handles both:
        - 1→1: [single_output]
        - 1→N: [output1, output2, output3, ...]

        Steps:
        1. Fingerprint input
        2. For each output: upload to IPFS, create DataPiece, create edge

        Args:
            input_data: Input to component
            output_data: List of outputs (even if just one)
            component_config: Component configuration
            processing_time_ms: Processing time
            content_type: FAIR semantic type (Document, Chunk, Embedding, DataNode)
            run_id: FAIR provenance - links outputs to a Run node

        Returns:
            {
                "input_fingerprint": str,
                "output_fingerprints": List[str],
                "output_ipfs_hashes": List[str]
            }

        Example:
            >>> # Single output with FAIR metadata
            >>> outgate.store(
            ...     markdown, [chunk], config,
            ...     content_type=ContentType.CHUNK,
            ...     run_id="run_abc123"
            ... )

            >>> # Multiple outputs
            >>> outgate.store(markdown, [chunk1, chunk2, chunk3], config)
        """
        # Hash config
        config_hash = self.hash_config(component_config)

        # Fingerprint input
        input_fingerprint = self.fingerprint_data(input_data)

        # Batch upload all outputs to Akave in parallel using ThreadPoolExecutor
        from concurrent.futures import ThreadPoolExecutor, as_completed

        output_records = []

        # Prepare data for parallel upload
        upload_tasks = []
        for idx, output_item in enumerate(output_data):
            output_fingerprint = self.fingerprint_data(output_item)
            data_type = type(output_item).__name__
            upload_tasks.append(
                {
                    "idx": idx,
                    "item": output_item,
                    "fingerprint": output_fingerprint,
                    "data_type": data_type,
                }
            )

        # Upload in parallel (max 10 concurrent uploads)
        with ThreadPoolExecutor(max_workers=min(10, len(upload_tasks))) as executor:
            future_to_task = {
                executor.submit(self._upload_to_storage, task["item"]): task
                for task in upload_tasks
            }

            for future in as_completed(future_to_task):
                task = future_to_task[future]
                object_key = future.result()

                record: Dict[str, Any] = {
                    "fingerprint": task["fingerprint"],
                    "ipfs_hash": object_key,
                    "data_type": task["data_type"],
                }

                # Add FAIR compliance fields if provided
                if content_type is not None:
                    record["content_type"] = content_type.value
                    record["uri"] = generate_uri(content_type, task["fingerprint"])

                output_records.append(record)

        # Store everything to Neo4j in one batch
        self.logger.info(
            f"Storing {len(output_records)} outputs to cache (component: {self.component_name})"
        )
        self.logger.info(
            f"Storage details: input_fp={input_fingerprint[:20]}..., "
            f"component_id={self.component_id}, config_hash={config_hash}"
        )
        self.graph_store.store_transformation_batch(
            input_fingerprint=input_fingerprint,
            input_ipfs_hash=self._upload_to_storage(input_data),
            input_data_type=type(input_data).__name__,
            output_records=output_records,
            component_id=self.component_id,
            component_name=self.component_name,
            config_hash=config_hash,
            username=self.username,
            processing_time_ms=processing_time_ms,
            run_id=run_id,
        )

        self.logger.debug(
            f"Stored to Neo4j with input fingerprint: {input_fingerprint}"
        )

        return {
            "input_fingerprint": input_fingerprint,
            "output_fingerprints": [r["fingerprint"] for r in output_records],
            "output_ipfs_hashes": [r["ipfs_hash"] for r in output_records],
        }

    async def store_async(
        self,
        input_data: Any,
        output_data: List[Any],
        component_config: Dict[str, Any],
        processing_time_ms: Optional[int] = None,
        content_type: Optional[ContentType] = None,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Async version of store.

        Store transformation: input → component → outputs.

        Args:
            input_data: Input to component
            output_data: List of outputs (even if just one)
            component_config: Component configuration
            processing_time_ms: Processing time
            content_type: FAIR semantic type (Document, Chunk, Embedding, DataNode)
            run_id: FAIR provenance - links outputs to a Run node

        Returns:
            {
                "input_fingerprint": str,
                "output_fingerprints": List[str],
                "output_ipfs_hashes": List[str]
            }
        """
        import asyncio

        # Hash config
        config_hash = self.hash_config(component_config)

        # Fingerprint input
        input_fingerprint = self.fingerprint_data(input_data)

        # Prepare metadata for all outputs
        output_metadata = []
        for output_item in output_data:
            output_fingerprint = self.fingerprint_data(output_item)
            data_type = type(output_item).__name__
            output_metadata.append(
                {
                    "item": output_item,
                    "fingerprint": output_fingerprint,
                    "data_type": data_type,
                }
            )

        # Batch upload all outputs to Akave in parallel using asyncio.gather
        async def upload_with_metadata(meta: dict) -> dict:
            object_key = await self._upload_to_storage_async(meta["item"])
            record: Dict[str, Any] = {
                "fingerprint": meta["fingerprint"],
                "ipfs_hash": object_key,
                "data_type": meta["data_type"],
            }
            if content_type is not None:
                record["content_type"] = content_type.value
                record["uri"] = generate_uri(content_type, meta["fingerprint"])
            return record

        # Upload all in parallel
        output_records = await asyncio.gather(
            *[upload_with_metadata(meta) for meta in output_metadata]
        )
        output_records = list(output_records)

        # Store everything to Neo4j in one batch (async)
        self.logger.info(
            f"Storing {len(output_records)} outputs to cache (async, component: {self.component_name})"
        )
        self.logger.info(
            f"Storage details: input_fp={input_fingerprint[:20]}..., "
            f"component_id={self.component_id}, config_hash={config_hash}"
        )
        await self.graph_store.store_transformation_batch_async(
            input_fingerprint=input_fingerprint,
            input_ipfs_hash=await self._upload_to_storage_async(input_data),
            input_data_type=type(input_data).__name__,
            output_records=output_records,
            component_id=self.component_id,
            component_name=self.component_name,
            config_hash=config_hash,
            username=self.username,
            processing_time_ms=processing_time_ms,
            run_id=run_id,
        )

        self.logger.debug(
            f"Stored to Neo4j (async) with input fingerprint: {input_fingerprint}"
        )

        return {
            "input_fingerprint": input_fingerprint,
            "output_fingerprints": [r["fingerprint"] for r in output_records],
            "output_ipfs_hashes": [r["ipfs_hash"] for r in output_records],
        }

    def fingerprint_data(self, data: Any) -> str:
        """Create SHA256 fingerprint of data."""
        data_str = self._serialize_for_fingerprint(data)
        hash_obj = hashlib.sha256(data_str.encode("utf-8"))
        return f"fp_{hash_obj.hexdigest()[:16]}"

    def hash_config(self, config: Dict[str, Any]) -> str:
        """Create hash of component configuration."""
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

    def _upload_to_storage(self, data: Any) -> str:
        """
        Upload data to Akave and return object key.

        Args:
            data: Any data type (Document, dict, str, bytes, etc.)

        Returns:
            Object key (stored as ipfs_hash for Neo4j compatibility)
        """
        try:
            result = self.storage_client.upload_any(data)
            object_key: str = result["Hash"]
            self.logger.debug(
                f"Akave upload successful: key={object_key}, Size={result.get('Size', 'unknown')}"
            )
            return object_key
        except Exception as e:
            self.logger.error(f"Akave upload failed: {type(e).__name__}: {str(e)}")
            raise

    async def _upload_to_storage_async(self, data: Any) -> str:
        """
        Async version of _upload_to_storage.

        Upload data to Akave and return object key.

        Args:
            data: Any data type (Document, dict, str, bytes, etc.)

        Returns:
            Object key (stored as ipfs_hash for Neo4j compatibility)
        """
        try:
            result = await self.storage_client.upload_any_async(data)
            object_key: str = result["Hash"]
            self.logger.debug(
                f"Akave upload successful (async): key={object_key}, Size={result.get('Size', 'unknown')}"
            )
            return object_key
        except Exception as e:
            self.logger.error(
                f"Akave upload failed (async): {type(e).__name__}: {str(e)}"
            )
            raise

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
