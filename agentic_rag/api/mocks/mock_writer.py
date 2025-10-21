"""Simplified mock writer component."""

import logging
import time
from typing import Any, Dict, List

from haystack import Document

logger = logging.getLogger(__name__)


class MockDocumentWriter:
    """Mock document writer with constant delay."""

    def __init__(self, delay: float = 0.0, **kwargs: Any) -> None:
        """Initialize the mock document writer.

        Args:
            delay: Constant delay in seconds to simulate processing time (default: 0.0)
            **kwargs: Other parameters (ignored)
        """
        self.delay = delay
        logger.info("MockDocumentWriter initialized with delay=%.3fs", delay)

    def run(self, documents: List[Document]) -> Dict[str, int]:
        """Write documents and return count.

        Args:
            documents: List of documents to write

        Returns:
            Dictionary with "documents_written" key containing the count
        """
        logger.info(
            "MockDocumentWriter.run() started with %d documents", len(documents)
        )

        if self.delay > 0:
            time.sleep(self.delay)

        documents_written = len(documents)
        logger.info(
            "MockDocumentWriter.run() completed: %d documents written",
            documents_written,
        )
        return {"documents_written": documents_written}
