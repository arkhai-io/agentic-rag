"""Embedding worker for processing chunks with GPU embedder."""

import logging
import time
from typing import Any, List

import torch.multiprocessing as mp
from haystack import Document

logger = logging.getLogger(__name__)


def simple_tokenize(text: str) -> int:
    """Simple whitespace-based tokenizer for counting tokens.

    This is a basic implementation that splits on whitespace.
    For production, use a proper tokenizer like HuggingFace.

    Args:
        text: Text to tokenize

    Returns:
        Number of tokens (words) in the text
    """
    return len(text.split())


class EmbeddingWorker:
    """Single worker process for embedding chunks with token-based dynamic batching."""

    def __init__(
        self,
        embedding_queue: mp.Queue,
        token_limit: int,
        wait_time: float = 0.01,
    ):
        """Initialize embedding worker.

        Args:
            embedding_queue: Queue to read chunks from
            token_limit: Maximum number of tokens per batch (dynamic batching)
            wait_time: Time to wait for additional items after first item (seconds)
        """
        self.embedding_queue = embedding_queue
        self.token_limit = token_limit
        self.wait_time = wait_time
        logger.info(
            "EmbeddingWorker initialized with token_limit=%d, wait_time=%.3fs",
            token_limit,
            wait_time,
        )

    def start_worker_loop(self, component_names: List[str]) -> None:
        """Start the embedding worker loop (runs in separate process).

        This function continuously:
        1. Collects chunks from embedding_queue until token limit is reached
        2. Processes batch through embedder (with internal GPU batching)
        3. Writes embedded documents to storage

        Args:
            component_names: List of component names [converter?, chunker, embedder, writer]
        """
        from agentic_rag.ingestion.api.mocks.mock_embedder import MockDocumentEmbedder
        from agentic_rag.ingestion.api.mocks.mock_writer import MockDocumentWriter

        logger.info(
            "EmbeddingWorker starting with token_limit=%d in process %s",
            self.token_limit,
            mp.current_process().name,
        )

        # Validate component count
        if len(component_names) not in (3, 4):
            raise ValueError(
                f"Pipeline must have 3 or 4 components, got {len(component_names)}"
            )

        # Instantiate embedder and writer (hardcoded for now)
        embedder = MockDocumentEmbedder(
            delay_per_batch=0.01,  # 10ms per batch (GPU concurrent processing)
        )
        writer = MockDocumentWriter(delay=0.001)  # Minimal delay

        logger.info("Embedder and writer initialized")

        try:
            while True:
                # Dynamic batching with corrected logic:
                # 1. Block waiting for first item
                # 2. Start timer
                # 3. Collect items (non-blocking) until EITHER:
                #    - Token limit reached, OR
                #    - wait_time elapsed AND queue empty
                # 4. Process batch
                # 5. Repeat

                chunk_buffer: List[Document] = []
                total_tokens = 0

                logger.info(
                    "Waiting for first chunk (blocking, token_limit=%d)...",
                    self.token_limit,
                )

                # STEP 1: Block waiting for first item
                item = self.embedding_queue.get(block=True, timeout=None)

                # Check for sentinel value to stop
                if item is None:
                    logger.info("Received stop signal")
                    logger.info("EmbeddingWorker stopping")
                    return

                chunk, metadata = item
                token_count = simple_tokenize(chunk.content or "")
                chunk_buffer.append(chunk)
                total_tokens += token_count

                logger.info(
                    "First chunk received: %s (%d tokens)",
                    chunk.id,
                    token_count,
                )

                # STEP 2: Start timer
                batch_start_time = time.time()

                # STEP 3: Collect additional items (non-blocking)
                while total_tokens < self.token_limit:
                    elapsed = time.time() - batch_start_time

                    # If wait_time has elapsed, check if queue is empty
                    if elapsed >= self.wait_time:
                        if self.embedding_queue.empty():
                            logger.info(
                                "Wait time elapsed (%.3fs) and queue empty, processing batch",
                                elapsed,
                            )
                            break
                        # Queue not empty, continue collecting

                    try:
                        # Non-blocking get
                        item = self.embedding_queue.get(block=False)

                        # Check for sentinel value to stop
                        if item is None:
                            logger.info("Received stop signal")

                            # Process remaining chunks in buffer
                            if chunk_buffer:
                                logger.info(
                                    "Processing final buffer of %d chunks (%d tokens)",
                                    len(chunk_buffer),
                                    total_tokens,
                                )
                                self._process_batch(chunk_buffer, embedder, writer)

                            logger.info("EmbeddingWorker stopping")
                            return

                        chunk, metadata = item
                        token_count = simple_tokenize(chunk.content or "")
                        chunk_buffer.append(chunk)
                        total_tokens += token_count

                        logger.info(
                            "Added chunk %s (%d tokens) to batch (total: %d/%d tokens)",
                            chunk.id,
                            token_count,
                            total_tokens,
                            self.token_limit,
                        )

                        # If we've reached the token limit, stop collecting
                        if total_tokens >= self.token_limit:
                            logger.info(
                                "Token limit reached (%d tokens), processing batch",
                                total_tokens,
                            )
                            break

                    except Exception:
                        # Queue is empty, small sleep to avoid busy waiting
                        time.sleep(0.001)  # 1ms
                        continue

                logger.info(
                    "Batch ready: %d chunks with %d total tokens (limit: %d), elapsed: %.3fs",
                    len(chunk_buffer),
                    total_tokens,
                    self.token_limit,
                    time.time() - batch_start_time,
                )

                # STEP 4: Process the batch
                self._process_batch(chunk_buffer, embedder, writer)

        except KeyboardInterrupt:
            logger.info("EmbeddingWorker interrupted by user")
        except Exception as e:
            logger.exception("EmbeddingWorker error: %s", e)
        finally:
            logger.info("EmbeddingWorker stopped")

    def _process_batch(
        self,
        chunks: List[Document],
        embedder: Any,
        writer: Any,
    ) -> None:
        """Process a batch of chunks through embedder and writer.

        Args:
            chunks: List of chunks to process
            embedder: Embedder component
            writer: Writer component
        """
        logger.info("Processing batch of %d chunks", len(chunks))

        # Embed chunks (embedder has internal GPU batching)
        embedder_result = embedder.run(chunks)
        embedded_docs = embedder_result["documents"]

        # Write embedded documents
        writer.run(embedded_docs)

        logger.info(
            "Batch complete: %d chunks embedded and written",
            len(embedded_docs),
        )


def start_embedding_worker(
    embedding_queue: mp.Queue,
    component_names: List[str],
    token_limit: int,
    wait_time: float = 0.01,
) -> None:
    """Entry point for embedding worker process.

    Args:
        embedding_queue: Queue to read chunks from
        component_names: List of component names for the pipeline
        token_limit: Maximum number of tokens per batch (dynamic batching)
        wait_time: Time to wait for additional items after first item (seconds)
    """
    worker = EmbeddingWorker(embedding_queue, token_limit, wait_time)
    worker.start_worker_loop(component_names)
