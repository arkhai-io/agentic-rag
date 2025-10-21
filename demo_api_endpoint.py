"""Demo script to test the /api/v1/ingest-papers endpoint.

This script creates mock PDF files and sends them to the API endpoint
to verify the pipeline integration works correctly.
"""

import json
import logging
import tempfile
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
    """Create a mock PDF with the specified number of pages.

    Args:
        num_pages: Number of pages to create

    Returns:
        BytesIO containing the PDF content
    """
    writer = PdfWriter()
    for i in range(num_pages):
        writer.add_blank_page(width=612, height=792)  # Letter size

    pdf_buffer = BytesIO()
    writer.write(pdf_buffer)
    pdf_buffer.seek(0)
    return pdf_buffer


def test_ingest_endpoint():
    """Test the /api/v1/ingest-papers endpoint."""
    # API endpoint
    url = "http://localhost:8000/api/v1/ingest-papers"

    # Create mock PDF files with different page counts
    pdf_files = [
        ("file1.pdf", create_mock_pdf(5)),
        ("file2.pdf", create_mock_pdf(10)),
        ("file3.pdf", create_mock_pdf(3)),
    ]

    # Haystack components configuration
    components_config = {
        "MARKER": {},  # Converter (GPU-based, 1s/page)
        "chunker": {},  # Chunker
        "embedder": {},  # Embedder (GPU-based, 1ms/chunk)
        "writer": {},  # Writer
    }

    # Prepare multipart form data
    files = [
        ("pdf_files", (filename, pdf_buffer, "application/pdf"))
        for filename, pdf_buffer in pdf_files
    ]

    data = {
        "haystack_components": json.dumps(components_config),
    }

    # Send request
    logger.info("Sending request to %s", url)
    logger.info("Files: %s", [f[1][0] for f in files])
    logger.info("Components: %s", list(components_config.keys()))

    try:
        response = requests.post(url, files=files, data=data)
        response.raise_for_status()

        result = response.json()
        logger.info("Response: %s", result)
        logger.info("Status: %s", result.get("message"))
        logger.info("Files received: %d", result.get("files_received"))

        # Wait a bit for workers to process
        logger.info("Waiting for workers to process documents...")
        logger.info("Check the server logs to see pipeline processing!")
        time.sleep(30)  # Give time for processing to complete

    except requests.exceptions.RequestException as e:
        logger.error("Request failed: %s", e)
        if hasattr(e.response, "text"):
            logger.error("Response: %s", e.response.text)


if __name__ == "__main__":
    logger.info("Starting API endpoint demo")
    logger.info("Make sure the FastAPI server is running on http://localhost:8000")
    logger.info("")

    # Wait a bit for user to start the server
    logger.info("Waiting 3 seconds for server to be ready...")
    time.sleep(3)

    test_ingest_endpoint()

    logger.info("Demo complete!")