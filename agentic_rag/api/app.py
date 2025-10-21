"""FastAPI application for agentic-rag."""

import json
import logging
import tempfile
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Optional

import torch.multiprocessing as mp
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from haystack import Document
from pypdf import PdfReader

from agentic_rag.api.models import BatchConfig, IngestResponse
from agentic_rag.core.converter_worker import start_conversion_worker
from agentic_rag.core.embedder_worker import start_embedding_worker
from agentic_rag.core.pipeline_queues import PipelineQueues

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global state for pipeline queues and worker processes
pipeline_queues: Optional[PipelineQueues] = None
conversion_worker_process: Optional[mp.Process] = None
embedding_worker_process: Optional[mp.Process] = None
batch_config: Optional[BatchConfig] = None
_workers_initialized: bool = False


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifespan context manager for starting/stopping worker processes."""
    global pipeline_queues, conversion_worker_process, embedding_worker_process, batch_config, _workers_initialized

    # Guard against multiple initializations (uvicorn reload, multiple workers, etc.)
    if _workers_initialized:
        logger.warning("Workers already initialized, skipping initialization")
        yield
        return

    _workers_initialized = True
    logger.info("Initializing workers (first initialization only)")

    # Set multiprocessing start method (required for PyTorch multiprocessing)
    try:
        mp.set_start_method("spawn", force=True)
    except RuntimeError:
        # Already set
        pass

    # Load batch configuration from environment
    batch_config = BatchConfig()
    logger.info("Batch configuration loaded: %s", batch_config.model_dump())

    # Initialize pipeline queues
    pipeline_queues = PipelineQueues(maxsize=0)
    logger.info("Pipeline queues initialized")

    # Start worker processes on app startup
    # Use placeholder component names - they'll be passed per-document via metadata
    component_names = ["converter", "chunker", "embedder", "writer"]

    logger.info("Starting conversion worker process...")
    conversion_worker_process = mp.Process(
        target=start_conversion_worker,
        args=(
            batch_config.conversion_worker_pool_size,
            pipeline_queues.conversion_queue,
            pipeline_queues.embedding_queue,
            component_names,
            batch_config.conversion_batch_page_limit,
            batch_config.conversion_batch_wait_time,
        ),
        name="ConversionWorker",
    )
    conversion_worker_process.start()
    logger.info(
        "Conversion worker process started (PID: %d)", conversion_worker_process.pid
    )

    logger.info("Starting embedding worker process...")
    embedding_worker_process = mp.Process(
        target=start_embedding_worker,
        args=(
            pipeline_queues.embedding_queue,
            component_names,
            batch_config.embedding_batch_token_limit,
            batch_config.embedding_batch_wait_time,
        ),
        name="EmbeddingWorker",
    )
    embedding_worker_process.start()
    logger.info(
        "Embedding worker process started (PID: %d)", embedding_worker_process.pid
    )

    yield

    # Cleanup: Send stop signal and wait for workers
    if pipeline_queues:
        logger.info("Shutting down workers...")
        pipeline_queues.conversion_queue.put(None)  # Sentinel to stop workers

    if conversion_worker_process:
        conversion_worker_process.join(timeout=10)
        if conversion_worker_process.is_alive():
            conversion_worker_process.terminate()

    if embedding_worker_process:
        embedding_worker_process.join(timeout=10)
        if embedding_worker_process.is_alive():
            embedding_worker_process.terminate()

    logger.info("Workers stopped")
    _workers_initialized = False


app = FastAPI(title="Agentic RAG API", version="0.1.0", lifespan=lifespan)


@app.get("/")
async def root() -> Dict[str, str]:
    """Root endpoint."""
    return {"message": "Agentic RAG API", "version": "0.1.0"}


@app.post("/api/v1/ingest-papers", response_model=IngestResponse)
async def ingest_papers(
    pdf_files: List[UploadFile] = File(..., description="PDF files to ingest"),
    haystack_components: str = Form(
        ..., description="JSON string of haystack component configuration"
    ),
) -> IngestResponse:
    """
    Ingest PDF papers with custom Haystack component pipeline.

    This endpoint enqueues documents to the conversion queue and returns immediately.
    Processing happens asynchronously in worker processes.

    Args:
        pdf_files: List of PDF files to process
        haystack_components: JSON string defining the Haystack pipeline components

    Returns:
        IngestResponse with submission confirmation
    """
    if pipeline_queues is None:
        raise HTTPException(status_code=500, detail="Pipeline queues not initialized")

    # Parse the haystack components JSON
    try:
        components_config: Dict[str, Any] = json.loads(haystack_components)
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid JSON in haystack_components: {e}",
        )

    # Process PDF files and enqueue to conversion queue
    for pdf_file in pdf_files:
        # Save to temporary file to extract page count
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
            content = await pdf_file.read()
            tmp_file.write(content)
            tmp_path = tmp_file.name

        try:
            # Extract page count
            reader = PdfReader(tmp_path)
            page_count = len(reader.pages)
            logger.info("File %s has %d pages", pdf_file.filename, page_count)

            # Create document and enqueue to conversion queue
            document = Document(
                content=tmp_path,  # Store temp file path for processing
                meta={
                    "filename": pdf_file.filename,
                    "page_count": page_count,
                    "components_config": components_config,
                },
            )

            pipeline_queues.enqueue_document(
                document,
                metadata={
                    "components_config": components_config,
                },
            )
            logger.info(
                "Enqueued document %s to conversion queue (page_count=%d)",
                pdf_file.filename,
                page_count,
            )

        except Exception as e:
            logger.exception("Failed to process file %s: %s", pdf_file.filename, e)
            # Continue with other files
            continue

    return IngestResponse(
        message=f"Successfully enqueued {len(pdf_files)} documents for processing",
        files_received=len(pdf_files),
    )


@app.get("/health")
async def health() -> Dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy"}
