"""Pydantic models for API requests and responses."""

from typing import Any, Dict, List

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


class DocumentItem(BaseModel):
    """Document item for conversion queue."""

    content_id: str = Field(..., description="ID to lookup document content in memory")
    filename: str = Field(..., description="Name of the PDF file")
    page_count: int = Field(..., gt=0, description="Number of pages in the document")


class ConversionBatch(BaseModel):
    """Batch of documents for conversion processing."""

    documents: List[DocumentItem] = Field(
        ..., description="List of documents with content IDs"
    )
    total_pages: int = Field(..., gt=0, description="Total number of pages in batch")
    components_config: Dict[str, Any] = Field(
        ..., description="Haystack components configuration for this batch"
    )


class ChunkItem(BaseModel):
    """Chunk item for embedding queue."""

    text: str = Field(..., description="Text content of the chunk")
    token_count: int = Field(..., gt=0, description="Number of tokens in the chunk")
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Metadata for the chunk"
    )


class EmbeddingBatch(BaseModel):
    """Batch of chunks for embedding processing."""

    chunks: List[ChunkItem] = Field(..., description="List of chunks with token counts")
    total_tokens: int = Field(..., gt=0, description="Total number of tokens in batch")
    components_config: Dict[str, Any] = Field(
        ..., description="Haystack components configuration for this batch"
    )


class IngestRequest(BaseModel):
    """Request model for ingesting papers (parsed from form data)."""

    components: Dict[str, Any] = Field(
        ..., description="Haystack component configuration"
    )


class IngestResponse(BaseModel):
    """Response model for ingest endpoint."""

    message: str = Field(..., description="Response message")
    files_received: int = Field(..., ge=0, description="Number of files received")
