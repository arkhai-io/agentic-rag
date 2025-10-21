"""Simplified mock embedder component."""

import logging
import time
from typing import Any, Dict, List

from haystack import Document

logger = logging.getLogger(__name__)


class MockDocumentEmbedder:
    """Mock document embedder that logs calls without processing input."""

    def __init__(self, delay: float = 0.0, **kwargs):
        """Initialize the mock document embedder.

        Args:
            delay: Delay in seconds to simulate processing time (default: 0.0)
            **kwargs: Other parameters (ignored)
        """
        self.delay = delay
        logger.info("MockDocumentEmbedder initialized with delay=%.3fs", delay)

    def run(self, documents: List[Document]) -> Dict[str, List[Document]]:
        """Log that embedder was called and return empty documents list."""
        logger.info("MockDocumentEmbedder.run() started")
        if self.delay > 0:
            time.sleep(self.delay)
        logger.info("MockDocumentEmbedder.run() completed")
        return {"documents": []}


class MockTextEmbedder:
    """Mock text embedder that logs calls without processing input."""

    def __init__(self, delay: float = 0.0, **kwargs):
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
