"""Dual pipeline queues for conversion and embedding stages using PyTorch multiprocessing."""

import logging
import queue
from typing import Any, Dict, Optional

import torch.multiprocessing as mp
from haystack import Document

logger = logging.getLogger(__name__)


class PipelineQueues:
    """Manages dual pipeline queues for conversion and embedding stages.

    - conversion_queue: Holds individual documents to be converted
    - embedding_queue: Holds individual chunks to be embedded
    """

    def __init__(self, maxsize: int = 0):
        """Initialize dual pipeline queues.

        Args:
            maxsize: Maximum size of each queue (0 for unlimited)
        """
        # Use PyTorch multiprocessing for GPU compatibility
        self.conversion_queue: mp.Queue = mp.Queue(maxsize=maxsize)
        self.embedding_queue: mp.Queue = mp.Queue(maxsize=maxsize)
        logger.info("PipelineQueues initialized with maxsize=%d (0=unlimited)", maxsize)

    def enqueue_document(
        self, document: Document, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add a document to the conversion queue.

        Args:
            document: Document to convert
            metadata: Optional metadata for processing
        """
        logger.info("Enqueueing document for conversion: %s", document.id)
        self.conversion_queue.put((document, metadata or {}))

    def dequeue_document(
        self, block: bool = True, timeout: Optional[float] = None
    ) -> Optional[tuple[Document, Dict[str, Any]]]:
        """Retrieve a document from the conversion queue.

        Args:
            block: Whether to block until an item is available
            timeout: Timeout in seconds (None for infinite)

        Returns:
            Tuple of (Document, metadata) if available, None if queue is empty and block=False
        """
        try:
            item = self.conversion_queue.get(block=block, timeout=timeout)
            document, metadata = item
            logger.info("Dequeued document for conversion: %s", document.id)
            return (document, metadata)
        except queue.Empty:
            return None

    def enqueue_chunk(
        self, chunk: Document, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add a chunk to the embedding queue.

        Args:
            chunk: Chunk to embed
            metadata: Optional metadata for processing
        """
        logger.info("Enqueueing chunk for embedding: %s", chunk.id)
        self.embedding_queue.put((chunk, metadata or {}))

    def dequeue_chunk(
        self, block: bool = True, timeout: Optional[float] = None
    ) -> Optional[tuple[Document, Dict[str, Any]]]:
        """Retrieve a chunk from the embedding queue.

        Args:
            block: Whether to block until an item is available
            timeout: Timeout in seconds (None for infinite)

        Returns:
            Tuple of (Document, metadata) if available, None if queue is empty and block=False
        """
        try:
            item = self.embedding_queue.get(block=block, timeout=timeout)
            chunk, metadata = item
            logger.info("Dequeued chunk for embedding: %s", chunk.id)
            return (chunk, metadata)
        except queue.Empty:
            return None

    def conversion_queue_size(self) -> int:
        """Get approximate size of conversion queue.

        Returns:
            Approximate number of items in queue
        """
        return self.conversion_queue.qsize()

    def embedding_queue_size(self) -> int:
        """Get approximate size of embedding queue.

        Returns:
            Approximate number of items in queue
        """
        return self.embedding_queue.qsize()

    def close(self) -> None:
        """Close both queues gracefully."""
        logger.info("Closing pipeline queues")
        self.conversion_queue.close()
        self.embedding_queue.close()

    def join_threads(self) -> None:
        """Wait for queue background threads to finish."""
        logger.info("Joining queue threads")
        self.conversion_queue.join_thread()
        self.embedding_queue.join_thread()
