"""Simplified mock converter component."""

import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from haystack import Document
from haystack.dataclasses import ByteStream

logger = logging.getLogger(__name__)


class MockConverter:
    """Mock converter that logs calls and propagates documents."""

    def __init__(
        self,
        delay_per_item: float = 0.0,
        batch_size: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the mock converter.

        Args:
            delay_per_item: Delay in seconds per item to simulate processing time (default: 0.0)
            batch_size: Process items in batches of this size (default: None, process all at once)
            **kwargs: Other parameters (ignored)
        """
        self.delay_per_item = delay_per_item
        self.batch_size = batch_size
        logger.info(
            "MockConverter initialized with delay_per_item=%.3fs, batch_size=%s",
            delay_per_item,
            batch_size,
        )

    def run(
        self,
        sources: List[Union[str, Path, ByteStream]],
        meta: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
    ) -> Dict[str, List[Document]]:
        """Log that converter was called and return mock documents.

        Args:
            sources: List of file paths, Path objects, or ByteStream objects
            meta: Optional metadata to attach to documents

        Returns:
            Dictionary with "documents" key containing converted documents
        """
        logger.info("MockConverter.run() started with %d sources", len(sources))

        # Simulate batch processing if configured
        if self.batch_size:
            num_batches = (len(sources) + self.batch_size - 1) // self.batch_size
            logger.info(
                "Processing %d batches of size %d", num_batches, self.batch_size
            )
            for i in range(num_batches):
                batch_start = i * self.batch_size
                batch_end = min((i + 1) * self.batch_size, len(sources))
                batch_size = batch_end - batch_start
                if self.delay_per_item > 0:
                    time.sleep(self.delay_per_item * batch_size)
                logger.info("Processed batch %d/%d", i + 1, num_batches)
        else:
            if self.delay_per_item > 0:
                time.sleep(self.delay_per_item * len(sources))

        # Create mock documents from sources
        documents: List[Document] = []
        for idx, source in enumerate(sources):
            # Create a mock document with minimal content
            source_str = str(source)
            doc_meta = {"source": source_str}

            # Merge metadata if provided
            if meta:
                if isinstance(meta, list):
                    doc_meta.update(meta[idx] if idx < len(meta) else {})
                else:
                    doc_meta.update(meta)

            documents.append(
                Document(
                    content=f"Mock converted content from {source_str}",
                    meta=doc_meta,
                )
            )

        doc_ids = [doc.id for doc in documents]
        logger.info(
            "MockConverter.run() completed with %d documents (IDs: %s)",
            len(documents),
            doc_ids,
        )
        return {"documents": documents}
