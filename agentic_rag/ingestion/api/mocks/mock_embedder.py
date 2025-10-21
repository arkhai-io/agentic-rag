"""Simplified mock embedder component."""

import logging
import math
import time
from typing import Any, Dict, List

from haystack import Document

logger = logging.getLogger(__name__)


class MockDocumentEmbedder:
    """Mock document embedder with batch processing and GPU simulation."""

    def __init__(
        self,
        delay_per_batch: float = 0.0,
        batch_size: int = 32,
        embedding_dim: int = 384,
        **kwargs: Any,
    ) -> None:
        """Initialize the mock document embedder.

        Args:
            delay_per_batch: Constant delay per batch (GPU processes batches concurrently, default: 0.0)
            batch_size: Number of documents to process per batch (default: 32)
            embedding_dim: Dimension of embedding vectors (default: 384)
            **kwargs: Other parameters (ignored)
        """
        self.delay_per_batch = delay_per_batch
        self.batch_size = batch_size
        self.embedding_dim = embedding_dim
        logger.info(
            "MockDocumentEmbedder initialized with delay_per_batch=%.3fs, batch_size=%d, embedding_dim=%d",
            delay_per_batch,
            batch_size,
            embedding_dim,
        )

    def run(self, documents: List[Document]) -> Dict[str, List[Document]]:
        """Embed documents in batches with constant delay per batch (GPU concurrent processing).

        Args:
            documents: List of documents to embed

        Returns:
            Dictionary with "documents" key containing documents with embeddings
        """
        input_doc_ids = [doc.id for doc in documents]
        logger.info(
            "MockDocumentEmbedder.run() started with %d documents (IDs: %s)",
            len(documents),
            input_doc_ids,
        )

        # Calculate number of batches
        num_batches = math.ceil(len(documents) / self.batch_size)
        logger.info(
            "Processing %d documents in %d batches of size %d",
            len(documents),
            num_batches,
            self.batch_size,
        )

        # Simulate batch processing with constant delay per batch (GPU concurrent processing)
        if self.delay_per_batch > 0:
            total_delay = self.delay_per_batch * num_batches
            time.sleep(total_delay)

        # Add mock embeddings to documents (in-place modification)
        embedded_docs = []
        for doc in documents:
            # Create a mock embedding vector (all zeros for simplicity)
            mock_embedding = [0.0] * self.embedding_dim

            # Create new document with embedding
            embedded_doc = Document(
                id=doc.id,
                content=doc.content,
                meta=doc.meta.copy() if doc.meta else {},
                embedding=mock_embedding,
            )

            embedded_docs.append(embedded_doc)

        output_doc_ids = [doc.id for doc in embedded_docs]
        logger.info(
            "MockDocumentEmbedder.run() completed with %d embedded documents (IDs: %s)",
            len(embedded_docs),
            output_doc_ids,
        )
        return {"documents": embedded_docs}


class MockTextEmbedder:
    """Mock text embedder that logs calls without processing input."""

    def __init__(self, delay: float = 0.0, **kwargs: Any) -> None:
        """Initialize the mock text embedder.

        Args:
            delay: Delay in seconds to simulate processing time (default: 0.0)
            **kwargs: Other parameters (ignored)
        """
        self.delay = delay
        logger.info("MockTextEmbedder initialized with delay=%.3fs", delay)

    def run(self, text: str) -> Dict[str, List[float]]:
        """Log that text embedder was called and return empty embedding."""
        logger.info("MockTextEmbedder.run() started")
        if self.delay > 0:
            time.sleep(self.delay)
        logger.info("MockTextEmbedder.run() completed")
        return {"embedding": []}
