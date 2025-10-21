"""Pydantic models for API requests and responses."""

from pydantic import BaseModel, Field


class BatchConfig(BaseModel):
    """Configuration for dynamic batching in conversion and embedding stages."""

    conversion_batch_page_limit: int = Field(
        default=100,
        gt=0,
        description="Maximum number of pages per conversion batch (dynamic batching by page count)",
    )
    conversion_worker_pool_size: int = Field(
        default=4,
        gt=0,
        description="Number of workers in conversion pool for parallel document processing",
    )
    embedding_batch_token_limit: int = Field(
        default=10000,
        gt=0,
        description="Maximum number of tokens per embedding batch (dynamic batching by token count)",
    )
