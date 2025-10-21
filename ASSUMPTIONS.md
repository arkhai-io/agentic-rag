# Implementation Assumptions for 1-Day initial prototype

## Key Simplifications

1. **Mock execution only** - No actual Haystack pipelines, just simulated delays (Marker: 1s/page, MarkItDown: 0.5s/page, Embedder: 1ms/chunk on GPU)

2. **In-memory queues** - Use PyTorch `multiprocessing.Queue` instances (one for conversion, one for embedding), no Redis/database persistence. Job state lost on restart.

3. **No VRAM management** - User responsible for configuring batch sizes to avoid OOM. Single GPU can handle both conversion and embedding workers in parallel.

4. **Basic whitespace tokenizer** - Use simple whitespace-based tokenizer for token counting in embedding dynamic batching (no HuggingFace dependencies for initial prototype).

5. **Multiprocessing workers** - Use PyTorch multiprocessing for converter and embedding workers to provide process isolation for black-box components. Architecture:
   - **Conversion Consumer Process**: Reads from queue and manages a persistent `multiprocessing.Pool` for parallel document processing within batches
   - **Conversion Worker Pool**: User-configurable fixed size (e.g., 2-4 workers) for concurrent PDF processing
   - **Embedding Worker Process**: Single process (no pool) since embedder has internal GPU batching
   - Main process handles dynamic batching logic before enqueueing

6. **Single GPU only** - Multi-GPU setup out of scope. If 1 GPU available, both converter and embedder workers can use it in parallel.

   **Component batch processing note**: Embedder (SentenceTransformers) natively supports true GPU batching. Marker PDF converter processes files sequentially in current implementation - it is assumed this will be fixed once we move from mocks to actual implementation.

7. **User-controlled batching and workers** - User provides configuration in request:
   - `conversion_batch_page_limit`: Max pages per conversion batch (dynamic batching by page count)
   - `conversion_worker_pool_size`: Fixed number of workers in conversion pool for parallel processing
   - `embedding_batch_token_limit`: Max tokens per embedding batch (dynamic batching by token count)
   No automatic batch optimization.

8. **Temp file storage** - PDFs saved to disk using `tempfile`.

9. **No authentication** - Open API, no auth for initial prototype.

10. **Integration tests only** - Target 60% coverage, focus on happy path (single job, queue, GPU vs CPU, batching).

## Out of Scope
- Multi-GPU support, real pipeline execution, VRAM tracking, persistent storage, job retry, advanced scheduling, metrics/monitoring, job cancellation, streaming responses, rate limiting
