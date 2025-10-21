# Implementation Assumptions for 1-Day initial prototype

## Key Simplifications

* Haystack components are mocked with basic implementations that simulate processing via delays and ensure a basic data flow.
* Tokenizer is emulated using a basic whitespace split.
* Only single GPU setup supported
* Pipelines can be eather of the two types:
  - conversion - chunking - embedding - writing
  - chunking - embedding - writing
* Chunking and writing are assumed to take negligible time, hence pipeline parallelism is simplified using only two queues.
* It is assumed haystack pipeline components have no heavyweight computations in the main Python process, and release GIL during processing.
* Conversion (for some components) and embedding (always) are the only two components that require GPU resources.
* Dynamic batching is used for both conversion (by page count) and embedding (by token count), but is simplified and waits for current batch to complete before starting a new one.
* It is assumed that conversion implementation supports batch processing of inputs. When using actual Marker converter instead of the mock, sequential processing will need to be replaced with a concurrent approach.
* User is responsible for configuring the following parameters via CLI or env vars to avoid OOM errors. It is assumed the user has tested their intended pipeline on the target GPU and found suitable parameters empirically.
   - `conversion_batch_page_limit`: Max pages per conversion batch (dynamic batching by page count)
   - `conversion_worker_pool_size`: Fixed number of workers in conversion pool for parallel processing
   - `embedding_batch_token_limit`: Max tokens per embedding batch (dynamic batching by token count)
* No persistent storage for jobs / queues. Job state lost on restart.
* No tracking of job statuses / results - ingestion is fire-and-forget as a demonstration.
* No ability to cancel running jobs.
* No test coverage - manual testing via log observation
* Error handling is not guaranteed, the implementation focuses on the happy path.
* No authentication or rate limiting.
