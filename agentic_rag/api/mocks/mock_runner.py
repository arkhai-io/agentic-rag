"""Simplified mock pipeline runner."""

import logging
from typing import Any, Dict, List

from haystack import Document

from agentic_rag.api.mocks.mock_chunker import MockChunker
from agentic_rag.api.mocks.mock_converter import MockConverter
from agentic_rag.api.mocks.mock_embedder import MockDocumentEmbedder
from agentic_rag.api.mocks.mock_writer import MockDocumentWriter

logger = logging.getLogger(__name__)


class MockPipelineRunner:
    """Mock pipeline runner that logs calls without processing input."""

    COMPONENT_MAP = {
        # WARNING: delays based on rough assumptions, not real benchmarks
        # Converters - 1s/page for Marker (GPU), 0.5s/page for MarkItDown (CPU)
        "CONVERTER.PDF": MockConverter(0.5),  # MarkItDown
        "CONVERTER.MARKER_PDF": MockConverter(1.0),  # Marker (GPU-accelerated)
        "CONVERTER.DOCX": MockConverter(0.3),  # haystack builtin
        "CONVERTER.HTML": MockConverter(0.2),  # haystack builtin
        "CONVERTER.TEXT": MockConverter(0.1),  # haystack builtin
        # Chunkers - minimal delay
        "CHUNKER.DOCUMENT_SPLITTER": MockChunker(0.001),
        "CHUNKER.MARKDOWN_AWARE": MockChunker(0.001),
        "CHUNKER.SEMANTIC": MockChunker(0.001),
        # Embedder - assume 10ms/batch (GPU-accelerated)
        "EMBEDDER.SENTENCE_TRANSFORMERS_DOC": MockDocumentEmbedder(0.01),
        # Writers - minimal delay
        "WRITER.DOCUMENT_WRITER": MockDocumentWriter(0.001),
    }

    def __init__(self, config: Any = None):
        """Initialize the mock pipeline runner (ignores config)."""
        logger.info("MockPipelineRunner initialized")
        self.components: Dict[str, Any] = {}
        self.pipeline_spec: List[str] = []

    def load_pipeline_spec(self, component_names: List[str]) -> None:
        """Log that pipeline spec was loaded and select pre-created components."""
        logger.info(
            "MockPipelineRunner.load_pipeline_spec() called with %d components",
            len(component_names),
        )
        self.pipeline_spec = component_names
        self.components = {name: self.COMPONENT_MAP[name] for name in component_names}

    def run(self, documents: List[Document]) -> Dict[str, Any]:
        """Log that pipeline run was called, execute components, and return result."""
        logger.info("MockPipelineRunner.run() called with %d documents", len(documents))

        # Simplified validation
        component_list = list(self.components.values())
        if len(component_list) == 3:
            converter = None
            chunker, embedder, writer = component_list
        elif len(component_list) == 4:
            converter, chunker, embedder, writer = component_list
        else:
            raise ValueError("Pipeline must have either 3 or 4 components.")

        # Execute pipeline stages
        if converter is None:
            converted: Dict[str, List[Document]] = {"documents": documents}
        else:
            converted = converter.run(sources=documents)

        chunked: Dict[str, List[Document]] = chunker.run(converted["documents"])
        embedded: Dict[str, List[Document]] = embedder.run(chunked["documents"])
        writer_result: Dict[str, Any] = writer.run(embedded["documents"])

        logger.info("MockPipelineRunner.run() completed")
        return writer_result
