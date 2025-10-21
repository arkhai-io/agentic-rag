# Implementation Plan: /api/v1/ingest-papers Endpoint

## Overview
Create async FastAPI endpoint with pipeline-parallel architecture using PyTorch multiprocessing queues for conversion and embedding stages, with dynamic batching based on user-configured limits. Uses persistent worker pool for conversion and single worker for embedding.

## Core Components

### 1. API Endpoint Enhancement
- Modify `/api/v1/ingest-papers` to accept:
  - PDF bytestream (multiple files)
  - Haystack components JSON config
  - User batching config: `conversion_batch_page_limit`, `conversion_worker_pool_size`, `embedding_batch_token_limit`
- Return job ID immediately (async processing)
- Add status endpoint `/api/v1/jobs/{job_id}`

### 2. Pipeline Queue System
- **Two-stage pipeline** with separate queues:
  - **Conversion Queue**: Holds batches of PDFs awaiting conversion
  - **Embedding Queue**: Holds chunks of text awaiting embedding
- Use PyTorch `multiprocessing.Queue` instances for both stages
- Job model: id, status, pdf_files, components_config, batch_config, created_at, progress
- **Process Architecture** (3 long-running processes + pool workers):
  1. **Main FastAPI Process**: Handles API requests, manages job state, performs dynamic batching before enqueueing
  2. **Conversion Consumer Process**: Reads from conversion queue, manages persistent `multiprocessing.Pool` (user-configurable size) for parallel document processing within batches
  3. **Embedding Worker Process**: Single process that reads from embedding queue and calls embedder (no pool needed since embedder has internal GPU batching)
- Job states: queued, converting, embedding, completed, failed
- Progress tracking: conversion_progress (pages done / total pages), embedding_progress (chunks done / total chunks)
- Multiprocessing provides process isolation for black-box converter and embedder components

### 3. Dynamic Batching System (Main Process)
- **Conversion Batching** (by page count):
  - Main process accumulates documents until page count reaches user's `conversion_batch_page_limit`
  - Enqueues ready-to-process batches to conversion queue
  - Conversion consumer process distributes documents in batch to pool workers for parallel processing
  - Track per-document page counts for accurate batching
- **Embedding Batching** (by token count):
  - Main process uses basic whitespace tokenizer for token counting per chunk (no HuggingFace dependencies)
  - Accumulates chunks until token count reaches user's `embedding_batch_token_limit`
  - Enqueues ready-to-process batches to embedding queue
  - Single embedding worker consumes batches and passes to embedder (which handles GPU batching internally)
- Track per-job data characteristics:
  - Total pages across PDFs
  - Chunks generated from conversion
  - Token count per chunk

### 4. Mock Component Execution
- Mock delays from ASSUMPTIONS.md:
  - Marker: 1s/page (GPU-accelerated)
  - MarkItDown: 0.5s/page (CPU-only)
  - Embedder: 1ms/chunk (GPU-accelerated)
- Simulate batching behavior with delays proportional to batch size
- Track which components use GPU (Marker, Embedder) vs CPU (MarkItDown)
- Single GPU can handle both converter and embedder workers in parallel
- Pipeline parallelism: While embedder processes chunks from job N, converter can process documents from job N+1

### 5. File Handling
- Save uploaded PDFs to `tempfile`
- Extract page count for processing estimation
- Cleanup temp files post-processing

### 6. Data Models
- JobSubmitRequest: files, components, batch_config
- JobSubmitResponse: job_id, status, conversion_queue_position, embedding_queue_position
- JobStatusResponse: job_id, status, conversion_progress, embedding_progress, result/error
- BatchConfig: conversion_batch_page_limit, conversion_worker_pool_size, embedding_batch_token_limit
- ConversionBatch: job_id, documents (with page counts), total_pages
- EmbeddingBatch: job_id, chunks (with token counts), total_tokens

## File Structure
```
agentic_rag/
├── api/
│   ├── app.py
│   ├── models.py (Pydantic models)
│   └── routes/
│       └── ingest.py
├── core/
│   ├── pipeline_queues.py (conversion + embedding queues)
│   ├── converter_worker.py (conversion worker pool)
│   ├── embedder_worker.py (embedding worker pool)
│   └── batching.py (dynamic batching logic for both stages)
└── services/
    └── mock_executor.py (mock component delays)
```

## Implementation Steps
1. Create Pydantic models for requests/responses/config (including batch models)
2. Build dual pipeline queues with PyTorch multiprocessing.Queue (conversion + embedding)
3. Implement dynamic batching system in main process:
   - Page-based batching for conversion (batches enqueued ready-to-process)
   - Token-based batching for embedding (batches enqueued ready-to-process, with basic whitespace tokenizer)
4. Create mock executor with simulated delays for both stages
5. Set up background worker processes:
   - Conversion consumer process (reads from conversion queue, manages persistent multiprocessing.Pool)
   - Single embedding worker process (reads from embedding queue, calls embedder directly)
6. Update API endpoints (submit + status with dual progress tracking)
7. Write integration tests (including pipeline parallelism and process isolation tests)

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
- Main process enqueues conversion batches (ready-to-process)
- Conversion consumer distributes documents to pool workers for parallel processing
- Pool workers push results back to conversion consumer, which enqueues chunks to embedding queue
- Embedding worker processes batches immediately when available
- All stages run concurrently for maximum throughput
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
