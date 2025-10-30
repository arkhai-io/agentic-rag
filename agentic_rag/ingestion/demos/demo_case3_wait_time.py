"""Demo Case 3: Wait Time Behavior - Batch Formation.

This script demonstrates that the wait_time parameter works correctly.
Workers wait briefly to accumulate items before processing, but will
process partial batches when the wait time elapses and queue is empty.

Expected behavior:
- Send documents with deliberate delays between them
- Worker waits for wait_time to collect more items
- When wait_time expires and queue is empty, process partial batch
- Logs should show "wait time elapsed and queue empty" messages
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


def run_case3_demo():
    """Run Case 3: Wait time behavior demonstration."""
    url = "http://localhost:8000/api/v1/ingest-papers"

    logger.info("=" * 80)
    logger.info("CASE 3: Wait Time Behavior - Batch Formation")
    logger.info("=" * 80)
    logger.info("Configuration:")
    logger.info("  - Wait time: 0.1s (default)")
    logger.info("  - Page limit: 100 pages (default)")
    logger.info("  - Strategy: Send documents in groups with delays")
    logger.info("")
    logger.info("Scenario:")
    logger.info("  1. Send 2 small docs (10p, 15p) = 25 pages < 100")
    logger.info("  2. Wait 0.5s (> wait_time)")
    logger.info("  3. Worker should process partial batch after wait_time expires")
    logger.info("  4. Send 2 more docs (20p, 12p) = 32 pages < 100")
    logger.info("  5. Wait 0.5s")
    logger.info("  6. Worker processes second partial batch")
    logger.info("=" * 80)

    # Use MARKER converter
    components_config = {
        "converter": "MARKER",
        "chunker": "DOCUMENT_SPLITTER",
        "embedder": "SENTENCE_TRANSFORMERS_DOC",
        "writer": "DOCUMENT_WRITER",
    }

    # Group 1: Small documents (will trigger wait time behavior)
    logger.info("")
    logger.info("📤 Sending Group 1: 2 documents (10p + 15p = 25 pages)...")

    pdf_files_group1 = [
        ("group1_doc1_10pages.pdf", create_mock_pdf(10)),
        ("group1_doc2_15pages.pdf", create_mock_pdf(15)),
    ]

    files_group1 = [
        ("pdf_files", (filename, pdf_buffer, "application/pdf"))
        for filename, pdf_buffer in pdf_files_group1
    ]

    data = {
        "haystack_components": json.dumps(components_config),
    }

    try:
        response = requests.post(url, files=files_group1, data=data)
        response.raise_for_status()
        result = response.json()
        logger.info("✓ Group 1 sent: %s", result.get("message"))

        logger.info("")
        logger.info("⏰ Waiting 0.5s (> wait_time of 0.1s)...")
        logger.info("   Worker should process Group 1 as partial batch...")
        time.sleep(0.5)

        # Group 2: Another set of small documents
        logger.info("")
        logger.info("📤 Sending Group 2: 2 documents (20p + 12p = 32 pages)...")

        pdf_files_group2 = [
            ("group2_doc1_20pages.pdf", create_mock_pdf(20)),
            ("group2_doc2_12pages.pdf", create_mock_pdf(12)),
        ]

        files_group2 = [
            ("pdf_files", (filename, pdf_buffer, "application/pdf"))
            for filename, pdf_buffer in pdf_files_group2
        ]

        response = requests.post(url, files=files_group2, data=data)
        response.raise_for_status()
        result = response.json()
        logger.info("✓ Group 2 sent: %s", result.get("message"))

        logger.info("")
        logger.info("⏰ Waiting 0.5s for Group 2 processing...")
        time.sleep(0.5)

        logger.info("")
        logger.info("Waiting for background processing to complete...")
        time.sleep(3)

        logger.info("")
        logger.info("=" * 80)
        logger.info("CASE 3 COMPLETE")
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
        "║                  GPU Concurrency Demo - Case 3                             ║"
    )
    logger.info(
        "║              Wait Time Behavior - Batch Formation                          ║"
    )
    logger.info(
        "╚════════════════════════════════════════════════════════════════════════════╝"
    )
    logger.info("")
    logger.info("Prerequisites:")
    logger.info("  1. FastAPI server must be running: agentic-rag-server")
    logger.info("  2. Server should have default batch config (wait_time=0.1s)")
    logger.info("")
    logger.info("This demo sends documents in separate groups with delays to trigger")
    logger.info("the wait_time behavior where partial batches are processed when the")
    logger.info("queue remains empty after the wait period expires.")
    logger.info("")

    logger.info("Waiting 2 seconds for server to be ready...")
    time.sleep(2)

    run_case3_demo()

    logger.info("")
    logger.info("Demo complete! Review the server logs to see:")
    logger.info("  - 'Wait time elapsed and queue empty' messages")
    logger.info("  - Partial batches being processed (< page_limit)")
    logger.info("  - Timestamps showing wait_time behavior")
    logger.info("")
