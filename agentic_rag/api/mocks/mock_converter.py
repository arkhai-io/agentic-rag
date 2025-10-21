"""Simplified mock converter component."""

import logging
import time
from typing import Any, Dict, List

from haystack import Document

logger = logging.getLogger(__name__)


class MockConverter:
    """Mock converter that logs calls without processing input."""

    def __init__(self, delay: float = 0.0, **kwargs):
        """Initialize the mock converter.

        Args:
            delay: Delay in seconds to simulate processing time (default: 0.0)
            **kwargs: Other parameters (ignored)
        """
        self.delay = delay
        logger.info("MockConverter initialized with delay=%.3fs", delay)

    def run(self, sources: Any, meta: Any = None) -> Dict[str, List[Document]]:
        """Log that converter was called and return empty documents list."""
        logger.info("MockConverter.run() started")
        if self.delay > 0:
            time.sleep(self.delay)
        logger.info("MockConverter.run() completed")
        return {"documents": []}
