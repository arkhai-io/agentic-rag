"""Demo Case 1: Sequential Batching - Conversion Worker.

This script demonstrates that the conversion worker correctly batches documents
based on page limits. Documents are batched when their combined page count
approaches the limit, then processed sequentially.

Expected behavior:
- Send 3 documents with 30, 40, and 50 pages (page_limit=100)
- First batch: doc1 (30) + doc2 (40) = 70 pages < 100
- Second batch: doc3 (50) = 50 pages < 100
- Logs should show dynamic batching and sequential processing
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


def create_mock_pdf(num_pages: int) -> BytesIO:
    """Create a mock PDF with the specified number of pages."""
    writer = PdfWriter()
    for i in range(num_pages):
        writer.add_blank_page(width=612, height=792)  # Letter size

    pdf_buffer = BytesIO()
    writer.write(pdf_buffer)
    pdf_buffer.seek(0)
    return pdf_buffer


def run_case1_demo():
    """Run Case 1: Sequential batching demonstration."""
    url = "http://localhost:8000/api/v1/ingest-papers"

    logger.info("=" * 80)
    logger.info("CASE 1: Sequential Batching - Conversion Worker")
    logger.info("=" * 80)
    logger.info("Configuration:")
    logger.info("  - Page limit: 100 pages (default)")
    logger.info("  - Documents: 3 PDFs with 30, 40, 50 pages")
    logger.info("  - Expected batches:")
    logger.info("    * Batch 1: doc1 (30p) + doc2 (40p) = 70 pages")
    logger.info("    * Batch 2: doc3 (50p) = 50 pages")
    logger.info("=" * 80)

    # Create mock PDF files with specific page counts
    pdf_files = [
        ("doc1_30pages.pdf", create_mock_pdf(30)),
        ("doc2_40pages.pdf", create_mock_pdf(40)),
        ("doc3_50pages.pdf", create_mock_pdf(50)),
    ]

    # Use MARKER converter (GPU-based, 1s/page simulated delay)
    components_config = {
        "converter": "MARKER",
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
    logger.info("Sending documents to ingestion endpoint...")

    try:
        response = requests.post(url, files=files, data=data)
        response.raise_for_status()

        result = response.json()
        logger.info("✓ Response: %s", result.get("message"))
        logger.info("✓ Files received: %d", result.get("files_received"))
        logger.info("")
        logger.info("Processing in background...")
        logger.info("Check server logs for batching behavior!")
        logger.info("")

        # Wait for processing to complete
        time.sleep(15)

        logger.info("=" * 80)
        logger.info("CASE 1 COMPLETE")
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
        "║                  GPU Concurrency Demo - Case 1                             ║"
    )
    logger.info(
        "║              Sequential Batching - Conversion Worker                       ║"
    )
    logger.info(
        "╚════════════════════════════════════════════════════════════════════════════╝"
    )
    logger.info("")
    logger.info("Prerequisites:")
    logger.info("  1. FastAPI server must be running: agentic-rag-server")
    logger.info("  2. Server should have default batch config (page_limit=100)")
    logger.info("")

    logger.info("Waiting 2 seconds for server to be ready...")
    time.sleep(2)

    run_case1_demo()

    logger.info("")
    logger.info("Demo complete! Review the server logs to see:")
    logger.info("  - Page counting and batch accumulation")
    logger.info("  - 'Page limit reached' or 'wait time elapsed' messages")
    logger.info("  - Sequential batch processing")
    logger.info("")
