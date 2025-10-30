"""
Demonstration of a basic mock pipeline running successfully.

This script shows how to:
1. Create mock documents
2. Configure a mock pipeline
3. Run the pipeline and see the results

WARNING: this doesn't use optimizations like batching or parallelism.
To see those in action, try demo_api_endpoint.py instead which tests the whole setup.
"""

import logging
from typing import List

from haystack import Document

from agentic_rag.ingestion.api.mocks import MockPipelineRunner

# Configure logging to see pipeline execution
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_sample_documents() -> List[Document]:
    """Create sample documents for testing."""
    documents = [
        Document(
            content="This is the first sample document about artificial intelligence.",
            meta={"source": "doc1.txt", "author": "Alice"},
        ),
        Document(
            content="The second document discusses machine learning algorithms.",
            meta={"source": "doc2.txt", "author": "Bob"},
        ),
        Document(
            content="Document three covers natural language processing techniques.",
            meta={"source": "doc3.txt", "author": "Charlie"},
        ),
    ]
    return documents


def demo_basic_pipeline():
    """Demonstrate a basic 3-component pipeline without converter."""
    logger.info("=" * 60)
    logger.info("DEMO: Basic 3-Component Pipeline (No Converter)")
    logger.info("=" * 60)

    # Create input documents
    documents = create_sample_documents()
    logger.info(f"Created {len(documents)} sample documents")

    # Configure the pipeline with component names
    component_names = [
        "CHUNKER.DOCUMENT_SPLITTER",
        "EMBEDDER.SENTENCE_TRANSFORMERS",
        "WRITER.CHROMA_DOCUMENT_WRITER",
    ]

    # Initialize and load the pipeline
    runner = MockPipelineRunner()
    runner.load_pipeline_spec(component_names)
    logger.info("\nStarting pipeline execution...")

    result = runner.run(documents)

    logger.info("\n" + "=" * 60)
    logger.info("Pipeline Execution Complete!")
    logger.info(f"Documents written: {result['documents_written']}")
    logger.info("=" * 60)

    return result


def demo_pipeline_with_converter():
    """Demonstrate a 4-component pipeline with converter."""
    logger.info("\n\n" + "=" * 60)
    logger.info("DEMO: 4-Component Pipeline (With PDF Converter)")
    logger.info("=" * 60)

    # Simulate file paths as input sources
    sources = ["sample1.pdf", "sample2.pdf"]
    logger.info(f"Input sources: {sources}")

    # Configure the pipeline with converter
    component_names = [
        "CONVERTER.PDF",
        "CHUNKER.DOCUMENT_SPLITTER",
        "EMBEDDER.SENTENCE_TRANSFORMERS",
        "WRITER.CHROMA_DOCUMENT_WRITER",
    ]

    # Initialize and load the pipeline
    runner = MockPipelineRunner()
    runner.load_pipeline_spec(component_names)
    logger.info("\nStarting pipeline execution...")

    result = runner.run(sources)

    logger.info("\n" + "=" * 60)
    logger.info("Pipeline Execution Complete!")
    logger.info(f"Documents written: {result['documents_written']}")
    logger.info("=" * 60)

    return result


def demo_gpu_accelerated_pipeline():
    """Demonstrate GPU-accelerated components with higher delays."""
    logger.info("\n\n" + "=" * 60)
    logger.info("DEMO: GPU-Accelerated Pipeline (Marker PDF)")
    logger.info("=" * 60)

    sources = ["complex_document.pdf"]
    logger.info(f"Input source: {sources}")

    # Configure pipeline with GPU-accelerated converter
    component_names = [
        "CONVERTER.MARKER_PDF",  # GPU-accelerated converter (1s delay)
        "CHUNKER.DOCUMENT_SPLITTER",
        "EMBEDDER.SENTENCE_TRANSFORMERS",
        "WRITER.CHROMA_DOCUMENT_WRITER",
    ]

    runner = MockPipelineRunner()
    runner.load_pipeline_spec(component_names)
    logger.info("\nStarting pipeline execution (with simulated GPU delay)...")

    result = runner.run(sources)

    logger.info("\n" + "=" * 60)
    logger.info("Pipeline Execution Complete!")
    logger.info(f"Documents written: {result['documents_written']}")
    logger.info("=" * 60)

    return result


def demo_pipeline_flow_details():
    """Demonstrate pipeline with detailed flow information."""
    logger.info("\n\n" + "=" * 60)
    logger.info("DEMO: Detailed Pipeline Flow Analysis")
    logger.info("=" * 60)

    documents = create_sample_documents()
    logger.info(f"Input: {len(documents)} documents")

    component_names = [
        "CHUNKER.DOCUMENT_SPLITTER",
        "EMBEDDER.SENTENCE_TRANSFORMERS",
        "WRITER.CHROMA_DOCUMENT_WRITER",
    ]

    runner = MockPipelineRunner()
    runner.load_pipeline_spec(component_names)

    # Show component configuration
    logger.info("\nPipeline Components:")
    component_list = list(runner.components.values())
    logger.info(f"  1. Chunker: {component_list[0].__class__.__name__}")
    logger.info(f"  2. Embedder: {component_list[1].__class__.__name__}")
    logger.info(f"  3. Writer: {component_list[2].__class__.__name__}")

    logger.info("\nExpected Flow:")
    logger.info("  Input: 3 documents")
    logger.info("  → Chunker: 3 docs × 3 chunks = 9 documents")
    logger.info("  → Embedder: 9 documents with embeddings (batch size: 32)")
    logger.info("  → Writer: 9 documents written")

    logger.info("\nExecuting pipeline...")
    result = runner.run(documents)

    logger.info("\n" + "=" * 60)
    logger.info("Actual Result:")
    logger.info(f"  Documents written: {result['documents_written']}")
    logger.info("=" * 60)

    return result


if __name__ == "__main__":
    print("\n")
    print("╔════════════════════════════════════════════════════════════╗")
    print("║      Mock Pipeline Demonstration - Successful Cases       ║")
    print("╚════════════════════════════════════════════════════════════╝")
    print("\n")

    # Run all demonstrations
    try:
        demo_basic_pipeline()
        demo_pipeline_with_converter()
        demo_gpu_accelerated_pipeline()
        demo_pipeline_flow_details()

        print("\n\n")
        print("╔════════════════════════════════════════════════════════════╗")
        print("║           All Demonstrations Completed Successfully!       ║")
        print("╚════════════════════════════════════════════════════════════╝")
        print("\n")

    except Exception as e:
        logger.error(f"Demo failed with error: {e}", exc_info=True)
        raise
