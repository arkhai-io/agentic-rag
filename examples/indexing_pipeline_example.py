"""
Indexing Pipeline Example

This example demonstrates how to create and run multiple indexing pipelines with
different configurations for optimal retrieval performance. We create:

1. Fast Pipeline: Small chunks with lightweight embeddings for quick retrieval
2. Semantic Pipeline: Larger chunks with powerful embeddings for deep understanding

Each pipeline:
- Converts documents (PDF/text)
- Chunks them with different strategies
- Generates embeddings using different models
- Stores them in separate ChromaDB collections

The pipelines automatically store metadata in Neo4j and content in Akave.
"""

import os
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

from agentic_rag import Config, PipelineFactory
from agentic_rag.components import GraphStore
from agentic_rag.pipeline import PipelineRunner

# Load environment variables from .env file
load_dotenv()

# Configuration
USERNAME = "your_username_2"
PROJECT = "demo_rag_app"  # Project name for organizing pipelines

# Pipeline names for different indexing strategies
FAST_PIPELINE = "fast_retrieval_index"
SEMANTIC_PIPELINE = "semantic_retrieval_index"

# Create configuration from environment variables
config = Config(
    neo4j_uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
    neo4j_username=os.getenv("NEO4J_USERNAME", "neo4j"),
    neo4j_password=os.getenv("NEO4J_PASSWORD"),
    akave_access_key=os.getenv("AKAVE_ACCESS_KEY"),  # For Akave S3-compatible storage
    akave_secret_key=os.getenv("AKAVE_SECRET_KEY"),
    akave_bucket=os.getenv("AKAVE_BUCKET", "agentic-rag"),
    log_level=os.getenv("AGENTIC_RAG_LOG_LEVEL", "INFO"),
)


def create_indexing_pipelines() -> List:
    """
    Create two indexing pipelines with different retrieval strategies.

    Pipeline 1 - Fast Retrieval:
    - Small chunks (300 chars) for precise matching
    - Lightweight embeddings (all-MiniLM-L6-v2) for speed
    - Optimized for quick, keyword-like retrieval

    Pipeline 2 - Semantic Retrieval:
    - Larger chunks (800 chars) for context preservation
    - Powerful embeddings (all-mpnet-base-v2) for semantic understanding
    - Optimized for conceptual and contextual retrieval

    Returns:
        List[PipelineSpec]: List of created pipeline specifications
    """
    # Initialize pipeline factory with config (singleton)
    # GraphStore will be created automatically from config
    factory = PipelineFactory(config=config)

    # Define component specs for both pipelines
    # Both pipelines use the same component types but different configurations
    pipeline_specs = [
        # Fast retrieval pipeline
        [
            {"type": "CONVERTER.TEXT"},
            {"type": "CHUNKER.MARKDOWN_AWARE"},
            {"type": "EMBEDDER.SENTENCE_TRANSFORMERS_DOC"},
            {"type": "WRITER.CHROMA_DOCUMENT_WRITER"},
        ],
        # Semantic retrieval pipeline
        [
            {"type": "CONVERTER.TEXT"},
            {"type": "CHUNKER.MARKDOWN_AWARE"},
            {"type": "EMBEDDER.SENTENCE_TRANSFORMERS_DOC"},
            {"type": "WRITER.CHROMA_DOCUMENT_WRITER"},
        ],
    ]

    # Configure each pipeline with different parameters
    # Note: root_dir is automatically generated as ./data/{username}/{pipeline_name}
    configs = [
        # Fast pipeline configuration
        {
            "_pipeline_name": FAST_PIPELINE,
            "markdown_aware_chunker": {
                "chunk_size": 300,  # Smaller chunks for precise retrieval
                "chunk_overlap": 30,  # 10% overlap
            },
            "document_embedder": {
                "model": "sentence-transformers/all-MiniLM-L6-v2",  # Fast, lightweight
            },
        },
        # Semantic pipeline configuration
        {
            "_pipeline_name": SEMANTIC_PIPELINE,
            "markdown_aware_chunker": {
                "chunk_size": 800,  # Larger chunks preserve context
                "chunk_overlap": 100,  # ~12% overlap
            },
            "document_embedder": {
                "model": "sentence-transformers/all-mpnet-base-v2",  # More powerful
            },
        },
    ]

    # Build both pipelines and store them in Neo4j
    # Each pipeline gets its own ChromaDB collection automatically
    # Username and project are injected at method level for multi-tenant isolation
    pipelines = factory.build_pipeline_graphs_from_specs(
        pipeline_specs=pipeline_specs,
        username=USERNAME,
        project=PROJECT,
        configs=configs,
        pipeline_types=["indexing", "indexing"],
    )

    if factory.graph_store:
        factory.graph_store.close()
    return pipelines


def run_indexing_pipelines(data_directory: str) -> Dict[str, Any]:
    """
    Run both indexing pipelines on documents in the specified directory.

    Args:
        data_directory: Path to directory containing documents (e.g., "./data/")

    Each pipeline will independently:
    1. Load the documents
    2. Chunk them according to its strategy
    3. Generate embeddings using its model
    4. Store everything in its own ChromaDB collection

    This allows the same documents to be indexed with multiple strategies
    for diverse retrieval capabilities.

    Returns:
        dict: Results from both pipelines
    """
    # Initialize GraphStore with config (singleton)
    graph_store = GraphStore(config=config)

    # Initialize runner (singleton - no username needed at init)
    # enable_caching=True stores content in Akave and enables transformation caching
    runner = PipelineRunner(
        graph_store=graph_store,
        enable_caching=True,  # Store data in Akave
        config=config,
    )

    # Load pipelines with username and project injection
    runner.load_pipelines(
        pipeline_names=[FAST_PIPELINE, SEMANTIC_PIPELINE],
        username=USERNAME,
        project=PROJECT,
    )

    results = {}

    # Run each indexing pipeline on the directory
    # Username and project are injected at method level
    for pipeline_name in [FAST_PIPELINE, SEMANTIC_PIPELINE]:
        result = runner.run(
            pipeline_name=pipeline_name,
            username=USERNAME,
            type="indexing",
            project=PROJECT,
            data_path=data_directory,
        )
        results[pipeline_name] = result

    graph_store.close()
    return results


if __name__ == "__main__":
    # Step 1: Create both indexing pipelines (only needs to be done once)
    # The pipeline structures are stored in Neo4j for reuse
    pipelines = create_indexing_pipelines()

    # Step 2: Run both pipelines on your documents
    # This can be called multiple times with different documents
    # Both pipelines will process the same documents with different strategies

    # Specify the data directory (runner will automatically find .txt, .pdf, .md files)
    data_dir = "./data/"

    # Verify directory exists and has files
    if not Path(data_dir).exists():
        print(f"WARNING: Directory {data_dir} not found")
        print("Please run: poetry run python examples/create_sample_data.py")
    else:
        # Check if directory has any supported files
        supported_files = list(Path(data_dir).glob("*.txt")) + list(
            Path(data_dir).glob("*.pdf")
        )
        if not supported_files:
            print(f"WARNING: No documents found in {data_dir}")
            print("Please run: poetry run python examples/create_sample_data.py")
        else:
            print(f"Indexing documents from {data_dir}...")
            results = run_indexing_pipelines(data_dir)
            print("Indexing complete!")
            # results["fast_retrieval_index"] contains fast pipeline results
            # results["semantic_retrieval_index"] contains semantic pipeline results
