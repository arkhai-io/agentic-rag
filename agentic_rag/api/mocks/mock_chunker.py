"""Simplified mock chunker component."""

import logging
import time
from typing import Any, Dict, List

from haystack import Document

logger = logging.getLogger(__name__)


class MockChunker:
    """Mock chunker that logs calls without processing input."""

    def __init__(self, delay: float = 0.0, **kwargs):
        """Initialize the mock chunker.

        Args:
            delay: Delay in seconds to simulate processing time (default: 0.0)
            **kwargs: Other parameters (ignored)
        """
        self.delay = delay
        logger.info("MockChunker initialized with delay=%.3fs", delay)

    def run(self, documents: List[Document]) -> Dict[str, List[Document]]:
        """Log that chunker was called and return empty documents list."""
        logger.info("MockChunker.run() started")
        if self.delay > 0:
            time.sleep(self.delay)
        logger.info("MockChunker.run() completed")
        return {"documents": []}
