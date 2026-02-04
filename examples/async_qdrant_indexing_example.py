"""
Async Qdrant Indexing Pipeline Example

This example demonstrates async indexing with Qdrant, which supports
async operations with BOTH local and remote storage (unlike ChromaDB).

Key advantage: Full async support with local file-based storage!

Pipelines:
1. Fast Pipeline: Small chunks + lightweight embeddings (384-dim)
2. Semantic Pipeline: Larger chunks + powerful embeddings (768-dim)

Qdrant stores collections locally with full folder hierarchy:
./data/{username}/{project}/{pipeline_name}/data/qdrant/
"""

import asyncio
import os
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

from agentic_rag import Config, PipelineFactory
from agentic_rag.components import GraphStore
from agentic_rag.pipeline import PipelineRunner

# Load environment variables
load_dotenv()

# Configuration
USERNAME = "your_username_2"
PROJECT = "demo_rag_app_qdrant"

# Pipeline names
FAST_PIPELINE = "qdrant_fast_index"
SEMANTIC_PIPELINE = "qdrant_semantic_index"

# Create configuration
config = Config(
    neo4j_uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
    neo4j_username=os.getenv("NEO4J_USERNAME", "neo4j"),
    neo4j_password=os.getenv("NEO4J_PASSWORD"),
    akave_access_key=os.getenv("AKAVE_ACCESS_KEY"),
    akave_secret_key=os.getenv("AKAVE_SECRET_KEY"),
    akave_bucket=os.getenv("AKAVE_BUCKET", "agentic-rag"),
    log_level=os.getenv("AGENTIC_RAG_LOG_LEVEL", "INFO"),
)


async def create_qdrant_indexing_pipelines_async() -> List:
    """
    Create two Qdrant indexing pipelines with different strategies.

    Qdrant advantage: Supports async with local storage!
    No Docker server required for async operations.
    """
    factory = PipelineFactory(config=config)

    # Define component specs - using Qdrant instead of Chroma
    pipeline_specs = [
        # Fast retrieval pipeline
        [
            {"type": "CONVERTER.TEXT"},
            {"type": "CHUNKER.MARKDOWN_AWARE"},
            {"type": "EMBEDDER.SENTENCE_TRANSFORMERS_DOC"},
            {"type": "WRITER.QDRANT_DOCUMENT_WRITER"},  # Qdrant writer
        ],
        # Semantic retrieval pipeline
        [
            {"type": "CONVERTER.TEXT"},
            {"type": "CHUNKER.MARKDOWN_AWARE"},
            {"type": "EMBEDDER.SENTENCE_TRANSFORMERS_DOC"},
            {"type": "WRITER.QDRANT_DOCUMENT_WRITER"},  # Qdrant writer
        ],
    ]

    # Configure each pipeline
    # Qdrant creates folder hierarchy: ./data/{username}/{project}/{pipeline}/data/qdrant/
    configs = [
        # Fast pipeline - 384 dimensions
        {
            "_pipeline_name": FAST_PIPELINE,
            "markdown_aware_chunker": {
                "chunk_size": 300,
                "chunk_overlap": 30,
            },
            "document_embedder": {
                "model": "sentence-transformers/all-MiniLM-L6-v2",  # 384-dim
            },
            "qdrant_document_writer": {
                # Local storage with async support!
                "qdrant_collection": FAST_PIPELINE,
                "embedding_dim": 384,  # Must match embedder output
            },
        },
        # Semantic pipeline - 768 dimensions
        {
            "_pipeline_name": SEMANTIC_PIPELINE,
            "markdown_aware_chunker": {
                "chunk_size": 800,
                "chunk_overlap": 100,
            },
            "document_embedder": {
                "model": "sentence-transformers/all-mpnet-base-v2",  # 768-dim
            },
            "qdrant_document_writer": {
                # Local storage with async support!
                "qdrant_collection": SEMANTIC_PIPELINE,
                "embedding_dim": 768,  # Must match embedder output
            },
        },
    ]

    # Build pipelines asynchronously
    pipelines = await factory.build_pipeline_graphs_from_specs_async(
        pipeline_specs=pipeline_specs,
        username=USERNAME,
        project=PROJECT,
        configs=configs,
        pipeline_types=["indexing", "indexing"],
    )

    if factory.graph_store:
        await factory.graph_store.close_async()
    return pipelines


async def run_qdrant_indexing_pipelines_async(data_directory: str) -> Dict[str, Any]:
    """
    Run both Qdrant indexing pipelines asynchronously.

    Stores embeddings in local Qdrant with full async support.
    """
    graph_store = GraphStore(config=config)

    # enable_caching=True stores content in Akave and enables transformation caching
    runner = PipelineRunner(
        graph_store=graph_store,
        enable_caching=True,  # Store data in Akave
        config=config,
    )

    # Load pipelines asynchronously
    await runner.load_pipelines_async(
        pipeline_names=[FAST_PIPELINE, SEMANTIC_PIPELINE],
        username=USERNAME,
        project=PROJECT,
    )

    results = {}

    # Run each pipeline asynchronously
    for pipeline_name in [FAST_PIPELINE, SEMANTIC_PIPELINE]:
        print(f"Starting async run for {pipeline_name}...")
        result = await runner.run_async(
            pipeline_name=pipeline_name,
            username=USERNAME,
            type="indexing",
            project=PROJECT,
            data_path=data_directory,
        )
        results[pipeline_name] = result

    await graph_store.close_async()
    return results


async def main() -> None:
    # Step 1: Create Qdrant pipelines
    print("Creating Qdrant pipelines asynchronously...")
    await create_qdrant_indexing_pipelines_async()
    print("Pipelines created.")

    # Step 2: Run pipelines on documents
    data_dir = "./data/"

    if not Path(data_dir).exists():
        print(f"WARNING: Directory {data_dir} not found")
        print("Please run: poetry run python examples/create_sample_data.py")
    else:
        supported_files = list(Path(data_dir).glob("*.txt")) + list(
            Path(data_dir).glob("*.pdf")
        )
        if not supported_files:
            print(f"WARNING: No documents found in {data_dir}")
            print("Please run: poetry run python examples/create_sample_data.py")
        else:
            print(f"Indexing documents from {data_dir} with Qdrant (local + async)...")
            await run_qdrant_indexing_pipelines_async(data_dir)
            print("Indexing complete!")
            print("\nQdrant collections created:")
            print(f"  - {FAST_PIPELINE} (384-dim embeddings)")
            print(f"  - {SEMANTIC_PIPELINE} (768-dim embeddings)")
            print(f"\nStored in: ./data/{USERNAME}/{PROJECT}/{{pipeline}}/data/qdrant/")


if __name__ == "__main__":
    asyncio.run(main())
