"""FastAPI application for agentic-rag."""

import json
from typing import Any, Dict, List

from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import JSONResponse

app = FastAPI(title="Agentic RAG API", version="0.1.0")


@app.get("/")
async def root() -> Dict[str, str]:
    """Root endpoint."""
    return {"message": "Agentic RAG API", "version": "0.1.0"}


@app.post("/api/v1/ingest-papers")
async def ingest_papers(
    pdf_files: List[UploadFile] = File(..., description="PDF files to ingest"),
    haystack_components: str = Form(
        ..., description="JSON string of haystack component configuration"
    ),
) -> JSONResponse:
    """
    Ingest PDF papers with custom Haystack component pipeline.

    Args:
        pdf_files: List of PDF files to process
        haystack_components: JSON string defining the Haystack pipeline components

    Returns:
        JSONResponse with processing results
    """
    # Parse the haystack components JSON
    try:
        components_config: Dict[str, Any] = json.loads(haystack_components)
    except json.JSONDecodeError as e:
        return JSONResponse(
            status_code=400,
            content={
                "error": "Invalid JSON in haystack_components",
                "details": str(e),
            },
        )

    # Mock: Process PDF files
    pdf_info = []
    for pdf_file in pdf_files:
        # Read file metadata (mocked - not actually processing)
        content_type = pdf_file.content_type
        filename = pdf_file.filename
        # In real implementation, would read: await pdf_file.read()

        pdf_info.append(
            {
                "filename": filename,
                "content_type": content_type,
                "status": "mocked_processing",
            }
        )

    # Mock: Process with Haystack components
    pipeline_result = {
        "pipeline_config": components_config,
        "components_received": list(components_config.keys()),
        "execution_status": "mocked",
        "message": "This is a mock response. Components would be executed here.",
    }

    # Return mock response
    return JSONResponse(
        status_code=200,
        content={
            "status": "success",
            "files_processed": len(pdf_files),
            "pdf_files": pdf_info,
            "pipeline_result": pipeline_result,
            "mock": True,
        },
    )


@app.get("/health")
async def health() -> Dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy"}
