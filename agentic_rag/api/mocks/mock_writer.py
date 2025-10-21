"""Simplified mock writer component."""

import logging
import time
from typing import Any, Dict, List

from haystack import Document

logger = logging.getLogger(__name__)


class MockDocumentWriter:
    """Mock document writer that logs calls without processing input."""

    def __init__(self, delay: float = 0.0, **kwargs):
        """Initialize the mock document writer.

        Args:
            delay: Delay in seconds to simulate processing time (default: 0.0)
            **kwargs: Other parameters (ignored)
        """
        self.delay = delay
        logger.info("MockDocumentWriter initialized with delay=%.3fs", delay)

    def run(self, documents: List[Document]) -> Dict[str, int]:
        """Log that writer was called and return 0 documents written."""
        logger.info("MockDocumentWriter.run() started")
        if self.delay > 0:
            time.sleep(self.delay)
        logger.info("MockDocumentWriter.run() completed")
        return {"documents_written": 0}
