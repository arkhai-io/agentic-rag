"""Simplified mock chunker component."""

import logging
import time
from typing import Any, Dict, List

from haystack import Document

logger = logging.getLogger(__name__)


class MockChunker:
    """Mock chunker that multiplies documents with chunk metadata."""

    def __init__(
        self, delay: float = 0.0, chunks_per_doc: int = 3, **kwargs: Any
    ) -> None:
        """Initialize the mock chunker.

        Args:
            delay: Constant delay in seconds to simulate processing time (default: 0.0)
            chunks_per_doc: Number of chunks to create per document (default: 3)
            **kwargs: Other parameters (ignored)
        """
        self.delay = delay
        self.chunks_per_doc = chunks_per_doc
        logger.info(
            "MockChunker initialized with delay=%.3fs, chunks_per_doc=%d",
            delay,
            chunks_per_doc,
        )

    def run(self, documents: List[Document]) -> Dict[str, List[Document]]:
        """Chunk documents and return with metadata.

        Args:
            documents: List of documents to chunk

        Returns:
            Dictionary with "documents" key containing chunked documents
        """
        input_doc_ids = [doc.id for doc in documents]
        logger.info(
            "MockChunker.run() started with %d documents (IDs: %s)",
            len(documents),
            input_doc_ids,
        )

        if self.delay > 0:
            time.sleep(self.delay)

        # Create chunks for each document
        chunks = []
        for doc in documents:
            for chunk_idx in range(self.chunks_per_doc):
                chunk_meta = doc.meta.copy() if doc.meta else {}
                chunk_meta.update(
                    {
                        "chunk_id": chunk_idx,
                        "total_chunks": self.chunks_per_doc,
                        "chunk_size": (
                            len(doc.content) // self.chunks_per_doc
                            if doc.content
                            else 0
                        ),
                    }
                )

                chunk_content = (
                    f"{doc.content} [chunk {chunk_idx + 1}/{self.chunks_per_doc}]"
                    if doc.content
                    else f"[chunk {chunk_idx + 1}/{self.chunks_per_doc}]"
                )

                chunk_id = (
                    f"{doc.id}_chunk_{chunk_idx}" if doc.id else f"chunk_{chunk_idx}"
                )
                chunks.append(
                    Document(
                        id=chunk_id,
                        content=chunk_content,
                        meta=chunk_meta,
                    )
                )

        chunk_ids = [chunk.id for chunk in chunks]
        logger.info(
            "MockChunker.run() completed: %d documents -> %d chunks (IDs: %s)",
            len(documents),
            len(chunks),
            chunk_ids,
        )
        return {"documents": chunks}
