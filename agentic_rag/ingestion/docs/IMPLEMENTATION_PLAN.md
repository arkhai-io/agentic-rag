# Implementation Plan: /api/v1/ingest-papers Endpoint

## Overview
Create async FastAPI endpoint with pipeline-parallel architecture using PyTorch multiprocessing queues for conversion and embedding stages, with dynamic batching based on user-configured limits. Uses single worker processes for both conversion and embedding stages.

## Core Components

### 1. API Endpoint (Prototype - No Job Tracking)
- Modify `/api/v1/ingest-papers` to accept:
  - PDF bytestream (multiple files)
  - Haystack components JSON config
  - User batching config: `conversion_batch_page_limit`, `conversion_worker_pool_size`, `embedding_batch_token_limit`
- Enqueue documents to conversion queue and return simple response
- NO job tracking, NO status endpoint in prototype (just fire and forget)

### 2. Pipeline Queue System (✅ IMPLEMENTED)
- **Two-stage pipeline** with separate queues (chunking and writing assumed to take negligible time):
  - **Conversion Queue**: Holds individual documents awaiting conversion (optional - bypassed if no conversion in pipeline)
  - **Embedding Queue**: Holds individual chunks awaiting embedding
- Use PyTorch `multiprocessing.Queue` instances for both stages
- **Pipeline Types Supported**:
  1. Full pipeline: conversion → chunking → embedding → writing
  2. Conversion-less pipeline: chunking → embedding → writing (documents go directly to embedding queue)
- **Process Architecture** (2 long-running worker processes):
  1. **Main FastAPI Process**: Handles API requests, enqueues individual documents to conversion queue
  2. **Conversion Worker Process**: Reads individual documents from conversion queue, dynamically batches them (✅ page-based batching), processes batch sequentially through converter, outputs individual chunks to embedding queue
  3. **Embedding Worker Process**: Single process that reads individual chunks from embedding queue, dynamically batches them (✅ token-based batching), calls embedder (has internal GPU batching)
- NO job tracking, NO progress tracking in prototype
- Multiprocessing provides process isolation for black-box converter and embedder components
- User-configured `conversion_worker_pool_size` parameter is propagated to mock converter (will be used by actual converter implementation for internal parallelism)

### 3. Dynamic Batching System (Worker Processes) (✅ IMPLEMENTED)
- **Conversion Batching** (by page count):
  - Main process enqueues individual documents to conversion queue
  - Conversion worker batching logic:
    1. Block waiting for first item (or sentinel)
    2. Start timer
    3. Keep collecting items (non-blocking with short timeout) until EITHER:
       - Total page count reaches `conversion_batch_page_limit`, OR
       - `conversion_batch_wait_time` has elapsed AND no more items available in queue
    4. Process the collected batch
    5. Wait for batch processing to complete before collecting next batch
  - Processes accumulated batch sequentially through converter + chunker (converter receives `conversion_worker_pool_size` for internal parallelism)
  - Outputs individual chunks to embedding queue
- **Embedding Batching** (by token count):
  - Conversion worker enqueues individual chunks to embedding queue
  - Embedding worker batching logic:
    1. Block waiting for first chunk (or sentinel)
    2. Start timer
    3. Keep collecting chunks (non-blocking with short timeout) until EITHER:
       - Total token count reaches `embedding_batch_token_limit`, OR
       - `embedding_batch_wait_time` has elapsed AND no more chunks available in queue
    4. Process the collected batch
    5. Wait for batch processing to complete before collecting next batch
  - Processes batch through embedder (which has internal GPU batching)
  - Uses basic whitespace tokenizer (`simple_tokenize()` function) for token counting per chunk (no HuggingFace dependencies)
  - Writes embedded documents to storage

### 4. Mock Component Execution (✅ IMPLEMENTED)
- Mock delays based on ASSUMPTIONS.md:
  - MARKER converter: 1s/page (GPU-accelerated)
  - MARKITDOWN converter: 0.5s/page (CPU-only)
  - Default converter: 0.1s/page
  - Embedder: 0.01s/batch (simulates GPU batch processing)
  - Chunker: 0.01s/call (creates 3 chunks per document)
  - Writer: 0.001s/call
- Mock components located in `api/mocks/` directory
- Component instantiation based on component name in metadata
- Single GPU can handle both converter and embedder workers in parallel
- Pipeline parallelism: While embedder processes chunks from job N, converter can process documents from job N+1

### 5. File Handling
- Save uploaded PDFs to `tempfile`
- Extract page count for processing estimation
- Cleanup temp files post-processing

### 6. Data Models (Actual Implementation)
**Configuration Models:**
- `BatchConfig`: Configuration loaded from environment variables (prefix `AGENTIC_RAG_`)
  - `conversion_batch_page_limit`: int (default: 100)
  - `conversion_worker_pool_size`: int (default: 4)
  - `conversion_batch_wait_time`: float (default: 0.1)
  - `embedding_batch_token_limit`: int (default: 10000)
  - `embedding_batch_wait_time`: float (default: 0.01)

**API Request/Response Models:**
- `IngestRequest`:
  - `components`: Dict[str, Any] (Haystack components config)
- `IngestResponse`:
  - `message`: str
  - `files_received`: int

**Queue Item Models:**
- `DocumentItem`:
  - `content_id`: str
  - `filename`: str
  - `page_count`: int
- `ChunkItem`:
  - `text`: str
  - `token_count`: int
  - `metadata`: Dict[str, Any]

**Batch Models (used internally by workers):**
- `ConversionBatch`:
  - `documents`: List[DocumentItem]
  - `total_pages`: int
  - `components_config`: Dict[str, Any]
- `EmbeddingBatch`:
  - `chunks`: List[ChunkItem]
  - `total_tokens`: int
  - `components_config`: Dict[str, Any]

**Note:** No job tracking models (JobSubmitResponse, JobStatusResponse) in prototype - fire-and-forget design.

## File Structure
```
agentic_rag/ingestion/
├── api/
│   ├── __init__.py (exports FastAPI app)
│   ├── app.py (FastAPI application with lifespan, all endpoints defined here)
│   ├── models.py (Pydantic models)
│   └── mocks/
│       ├── __init__.py
│       ├── mock_converter.py
│       ├── mock_chunker.py
│       ├── mock_embedder.py
│       └── mock_writer.py
├── core/
│   ├── __init__.py
│   ├── pipeline_queues.py (dual queue system with PyTorch multiprocessing)
│   ├── converter_worker.py (conversion worker with embedded batching logic)
│   └── embedder_worker.py (embedding worker with embedded batching logic)
├── cli.py (CLI entry point for starting server)
├── demos/
│   ├── __init__.py
│   ├── demo_api_endpoint.py (test script for API)
│   └── demo_mock_pipeline.py
└── docs/
    └── IMPLEMENTATION_PLAN.md
```

## Implementation Steps
1. ✅ Create Pydantic models for requests/responses/config (including batch models)
2. ✅ Build dual pipeline queues with PyTorch multiprocessing.Queue (conversion + embedding)
   - Queues hold individual items (documents and chunks), not batches
3. ✅ Set up background worker processes:
   - Conversion worker process (reads individual documents, batches and processes sequentially)
   - Embedding worker process (reads individual chunks, batches and processes sequentially)
4. ✅ Update API endpoint (simple submit, no job tracking for prototype)
5. ✅ Implement dynamic batching in worker processes:
   - ✅ Page-based batching for conversion worker (collect documents until page limit reached)
   - ✅ Token-based batching for embedding worker (collect chunks until token limit reached, with basic whitespace tokenizer)
6. ✅ Manual testing with logs (verify batching and pipeline parallelism)
   - Demo script created: `demos/demo_api_endpoint.py`

## Dynamic Batching Logic

### Conversion Stage
- Accumulate documents until total page count reaches `conversion_batch_page_limit`
- Process batch through converter (simulated delay: 1s/page for Marker, 0.5s/page for MarkItDown)
- Output chunks to embedding queue
- Track progress: pages processed / total pages

### Embedding Stage
- Mock tokenizer by using a basic implementation whitespace-based tokenizer, don't load full HF tokenizer for initial prototype
- Accumulate chunks until total token count reaches `embedding_batch_token_limit`
- Process batch through embedder (simulated delay: 1ms/chunk on GPU)
- Output embeddings to results
- Track progress: chunks processed / total chunks

### Pipeline Coordination
- Main process enqueues individual documents to conversion queue
- Conversion worker accumulates documents into batches, processes each batch sequentially (waits for batch completion), then enqueues individual chunks to embedding queue
- Embedding worker accumulates chunks into batches, processes each batch sequentially (waits for batch completion), then writes results
- Pipeline parallelism enabled: while embedding worker processes batch N, conversion worker can process batch N+1
- No GPU resource decisions - user controls batch sizes and worker pool size to avoid OOM

## Testing Strategy
- Single job with batching on both stages
- Multiple jobs flowing through pipeline (verify parallelism)
- Page limit enforcement for conversion batching
- Token limit enforcement for embedding batching
- Worker pool size configuration
- Pipeline coordination (converter produces while embedder consumes)
- Happy path only, don't test failure/retry logic for initial prototype
- Don't write tests, rely on logs to determine correctness of batching and parallelism
