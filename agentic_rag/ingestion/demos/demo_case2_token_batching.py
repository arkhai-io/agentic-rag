"""Demo Case 2: Dynamic Token-Based Batching - Embedding Worker.

This script demonstrates that the embedding worker correctly batches chunks
based on token limits. No converter is used in the pipeline, so documents
go directly to chunking and embedding.

Expected behavior:
- Pipeline: [chunker, embedder, writer] (NO converter)
- Send documents that skip conversion and go directly to chunking
- Embedding worker accumulates chunks until token_limit is reached
- Logs should show token counting and dynamic batch formation
"""

import json
import logging
import time
from io import BytesIO

import requests
from pypdf import PdfWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_mock_pdf_with_content(num_pages: int, content_per_page: str) -> BytesIO:
    """Create a mock PDF with the specified number of pages."""
    writer = PdfWriter()
    for i in range(num_pages):
        writer.add_blank_page(width=612, height=792)  # Letter size

    pdf_buffer = BytesIO()
    writer.write(pdf_buffer)
    pdf_buffer.seek(0)
    return pdf_buffer


def run_case2_demo():
    """Run Case 2: Token-based batching demonstration."""
    url = "http://localhost:8000/api/v1/ingest-papers"

    logger.info("=" * 80)
    logger.info("CASE 2: Dynamic Token-Based Batching - Embedding Worker")
    logger.info("=" * 80)
    logger.info("Configuration:")
    logger.info("  - Pipeline: [chunker, embedder, writer] (NO converter)")
    logger.info("  - Token limit: 10000 tokens (default)")
    logger.info("  - Documents: Multiple PDFs with varying page counts")
    logger.info("  - Expected: Dynamic batching based on chunk token counts")
    logger.info("=" * 80)

    # Create documents with varying page counts (pages will be chunked)
    # Each page produces ~3 chunks, each chunk ~150-200 tokens (estimated)
    pdf_files = [
        (
            "doc1_5pages.pdf",
            create_mock_pdf_with_content(5, "content"),
        ),  # ~15 chunks, ~2250 tokens
        (
            "doc2_3pages.pdf",
            create_mock_pdf_with_content(3, "content"),
        ),  # ~9 chunks, ~1350 tokens
        (
            "doc3_7pages.pdf",
            create_mock_pdf_with_content(7, "content"),
        ),  # ~21 chunks, ~3150 tokens
        (
            "doc4_4pages.pdf",
            create_mock_pdf_with_content(4, "content"),
        ),  # ~12 chunks, ~1800 tokens
        (
            "doc5_6pages.pdf",
            create_mock_pdf_with_content(6, "content"),
        ),  # ~18 chunks, ~2700 tokens
    ]

    # NO converter in the pipeline - documents skip conversion stage
    components_config = {
        "chunker": "DOCUMENT_SPLITTER",
        "embedder": "SENTENCE_TRANSFORMERS_DOC",
        "writer": "DOCUMENT_WRITER",
    }

    files = [
        ("pdf_files", (filename, pdf_buffer, "application/pdf"))
        for filename, pdf_buffer in pdf_files
    ]

    data = {
        "haystack_components": json.dumps(components_config),
    }

    logger.info("")
    logger.info("Sending documents to ingestion endpoint (no converter)...")
    logger.info("Expected: ~75 total chunks, ~11,250 tokens total")
    logger.info(
        "Token batching should create multiple batches based on 10k token limit"
    )
    logger.info("")

    try:
        response = requests.post(url, files=files, data=data)
        response.raise_for_status()

        result = response.json()
        logger.info("✓ Response: %s", result.get("message"))
        logger.info("✓ Files received: %d", result.get("files_received"))
        logger.info("")
        logger.info("Processing in background...")
        logger.info("Check server logs for token-based batching behavior!")
        logger.info("")

        # Wait for processing to complete
        time.sleep(10)

        logger.info("=" * 80)
        logger.info("CASE 2 COMPLETE")
        logger.info("=" * 80)

    except requests.exceptions.RequestException as e:
        logger.error("Request failed: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.error("Response: %s", e.response.text)


if __name__ == "__main__":
    logger.info("")
    logger.info(
        "╔════════════════════════════════════════════════════════════════════════════╗"
    )
    logger.info(
        "║                  GPU Concurrency Demo - Case 2                             ║"
    )
    logger.info(
        "║         Dynamic Token-Based Batching - Embedding Worker                   ║"
    )
    logger.info(
        "╚════════════════════════════════════════════════════════════════════════════╝"
    )
    logger.info("")
    logger.info("Prerequisites:")
    logger.info("  1. FastAPI server must be running: agentic-rag-server")
    logger.info("  2. Server should have default batch config (token_limit=10000)")
    logger.info("")
    logger.info(
        "This demo sends documents WITHOUT a converter to show embedding worker"
    )
    logger.info("token-based batching in action.")
    logger.info("")

    logger.info("Waiting 2 seconds for server to be ready...")
    time.sleep(2)

    run_case2_demo()

    logger.info("")
    logger.info("Demo complete! Review the server logs to see:")
    logger.info("  - Token counting per chunk")
    logger.info("  - Dynamic batch accumulation")
    logger.info("  - 'Token limit reached' or 'wait time elapsed' triggers")
    logger.info("")
