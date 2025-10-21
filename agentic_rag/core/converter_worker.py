"""Conversion worker for sequential batch processing with internal converter parallelism."""

import logging
import time
from queue import Empty
from typing import Any, Dict, List, Tuple

import torch.multiprocessing as mp
from haystack import Document

logger = logging.getLogger(__name__)


class ConversionWorker:
    """Single worker process for sequential batch conversion with internal converter parallelism."""

    def __init__(
        self,
        pool_size: int,
        conversion_queue: mp.Queue,
        embedding_queue: mp.Queue,
        page_limit: int,
        wait_time: float,
    ):
        """Initialize conversion worker.

        Args:
            pool_size: Number of workers for converter's internal parallelism
            conversion_queue: Queue to read documents from
            embedding_queue: Queue to write chunks to
            page_limit: Maximum number of pages per batch (dynamic batching)
            wait_time: Minimum time (seconds) to wait before processing batch
        """
        self.pool_size = pool_size
        self.conversion_queue = conversion_queue
        self.embedding_queue = embedding_queue
        self.page_limit = page_limit
        self.wait_time = wait_time
        logger.info(
            "ConversionWorker initialized with pool_size=%d (for converter internal parallelism), page_limit=%d, wait_time=%.3fs",
            pool_size,
            page_limit,
            wait_time,
        )

    def start_worker_loop(self, component_names: List[str]) -> None:
        """Start the conversion worker loop (runs in separate process).

        This function continuously:
        1. Collects documents from conversion_queue until page limit is reached
        2. Processes batch sequentially through converter (which has internal parallelism)
        3. Enqueues chunks to embedding_queue

        Args:
            component_names: List of component names for the pipeline
        """
        from agentic_rag.api.mocks.mock_chunker import MockChunker
        from agentic_rag.api.mocks.mock_converter import MockConverter

        logger.info(
            "ConversionWorker starting with pool_size=%d (for converter), page_limit=%d in process %s",
            self.pool_size,
            self.page_limit,
            mp.current_process().name,
        )

        # Determine if we have a converter based on component count
        if len(component_names) == 3:
            # No converter: [chunker, embedder, writer]
            converter = None
        elif len(component_names) == 4:
            # With converter: [converter, chunker, embedder, writer]
            converter_name = component_names[0]

            # Instantiate converter based on name with pool_size for internal parallelism
            if "MARKER" in converter_name:
                converter = MockConverter(
                    delay_per_item=1.0,  # Marker: 1s/page
                    pool_size=self.pool_size,
                )
            elif "MARKITDOWN" in converter_name:
                converter = MockConverter(
                    delay_per_item=0.5,  # MarkItDown: 0.5s/page
                    pool_size=self.pool_size,
                )
            else:
                converter = MockConverter(
                    delay_per_item=0.1,  # Default converter
                    pool_size=self.pool_size,
                )
        else:
            raise ValueError(
                f"Pipeline must have 3 or 4 components, got {len(component_names)}"
            )

        # Instantiate chunker
        chunker = MockChunker(delay=0.001, chunks_per_doc=3)

        logger.info("Converter and chunker initialized")

        try:
            while True:
                # Dynamic batching with wait time
                documents_to_process: List[Tuple[Document, Dict[str, Any]]] = []
                total_pages = 0

                logger.info(
                    "Waiting for documents (page_limit=%d, wait_time=%.3fs)...",
                    self.page_limit,
                    self.wait_time,
                )

                # 1. Block waiting for first item
                item = self.conversion_queue.get(block=True, timeout=None)

                # Check for sentinel value to stop
                if item is None:
                    logger.info("Received stop signal (queue empty)")
                    # Re-enqueue sentinel for other potential workers
                    self.conversion_queue.put(None)
                    # Signal embedding worker to stop
                    self.embedding_queue.put(None)
                    return

                # Add first item
                document, metadata = item
                page_count = document.meta.get("page_count", 1)
                documents_to_process.append((document, metadata))
                total_pages += page_count

                logger.info(
                    "Received first document %s (%d pages), starting timer",
                    document.meta.get("filename", document.id),
                    page_count,
                )

                # 2. Start timer
                start_time = time.time()

                # 3. Keep collecting items until either:
                #    - We reach the page limit, OR
                #    - wait_time has elapsed AND queue is empty
                while total_pages < self.page_limit:
                    elapsed = time.time() - start_time
                    remaining_wait = self.wait_time - elapsed

                    if remaining_wait <= 0:
                        # Wait time has elapsed, try non-blocking get
                        try:
                            item = self.conversion_queue.get(block=False)
                        except Empty:
                            # No more items available, process what we have
                            logger.info(
                                "Wait time elapsed and queue empty, processing batch"
                            )
                            break
                    else:
                        # Still within wait time, use short timeout
                        try:
                            item = self.conversion_queue.get(
                                block=True, timeout=min(remaining_wait, 0.01)
                            )
                        except Empty:
                            # Timeout, continue loop to check elapsed time
                            continue

                    # Check for sentinel value to stop
                    if item is None:
                        logger.info("Received stop signal while collecting")
                        # Re-enqueue sentinel
                        self.conversion_queue.put(None)
                        # Process collected documents before stopping
                        if documents_to_process:
                            logger.info(
                                "Processing final batch of %d documents (%d pages)",
                                len(documents_to_process),
                                total_pages,
                            )
                            self._process_batch(
                                documents_to_process, converter, chunker
                            )
                        # Signal embedding worker to stop
                        self.embedding_queue.put(None)
                        return

                    document, metadata = item
                    page_count = document.meta.get("page_count", 1)
                    documents_to_process.append((document, metadata))
                    total_pages += page_count

                    logger.info(
                        "Added document %s (%d pages) to batch (total: %d/%d pages, elapsed: %.3fs)",
                        document.meta.get("filename", document.id),
                        page_count,
                        total_pages,
                        self.page_limit,
                        elapsed,
                    )

                    # If we've reached the page limit, stop collecting
                    if total_pages >= self.page_limit:
                        logger.info("Page limit reached, processing batch")
                        break

                logger.info(
                    "Collected batch: %d documents with %d total pages (limit: %d)",
                    len(documents_to_process),
                    total_pages,
                    self.page_limit,
                )

                # 4. Process the batch
                self._process_batch(documents_to_process, converter, chunker)

        except KeyboardInterrupt:
            logger.info("ConversionWorker interrupted by user")
        except Exception as e:
            logger.exception("ConversionWorker error: %s", e)
        finally:
            logger.info("ConversionWorker stopped")

    def _process_batch(
        self,
        documents: List[Tuple[Document, Dict[str, Any]]],
        converter: Any,
        chunker: Any,
    ) -> None:
        """Process a batch of documents sequentially through converter and chunker.

        The converter has internal parallelism (pool_size workers).

        Args:
            documents: List of (document, metadata) tuples to process
            converter: Converter component (or None if no conversion)
            chunker: Chunker component
        """
        logger.info(
            "Processing batch of %d documents sequentially through converter and chunker",
            len(documents),
        )

        all_chunks = []

        # Process each document sequentially
        for document, metadata in documents:
            # Convert document if converter exists
            if converter is None:
                converted_docs = [document]
            else:
                converter_result = converter.run(sources=[document.content or ""])
                converted_docs = converter_result["documents"]

            # Chunk documents
            chunker_result = chunker.run(converted_docs)
            chunks = chunker_result["documents"]
            all_chunks.extend(chunks)

            logger.info(
                "Processed document %s -> %d chunks",
                document.meta.get("filename", document.id),
                len(chunks),
            )

        # Enqueue all chunks to embedding queue
        for chunk in all_chunks:
            self.embedding_queue.put((chunk, {}))

        logger.info(
            "Batch complete: processed %d documents -> %d total chunks enqueued",
            len(documents),
            len(all_chunks),
        )


def start_conversion_worker(
    pool_size: int,
    conversion_queue: mp.Queue,
    embedding_queue: mp.Queue,
    component_names: List[str],
    page_limit: int,
    wait_time: float,
) -> None:
    """Entry point for conversion worker process.

    Args:
        pool_size: Number of workers in the pool
        conversion_queue: Queue to read documents from
        embedding_queue: Queue to write chunks to
        component_names: List of component names for the pipeline
        page_limit: Maximum number of pages per batch (dynamic batching)
        wait_time: Minimum time (seconds) to wait before processing batch
    """
    worker = ConversionWorker(
        pool_size, conversion_queue, embedding_queue, page_limit, wait_time
    )
    worker.start_worker_loop(component_names)
