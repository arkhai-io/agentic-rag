# Agentic RAG Examples

## Setup

```bash
poetry install
```

Configure `.env`:
```bash
NEO4J_URI=your_neo4j_uri
NEO4J_USERNAME=your_username
NEO4J_PASSWORD=your_password
OPENROUTER_API_KEY=your_key
```

## Usage

```bash
# Create sample data
poetry run python examples/create_sample_data.py

# Index documents
poetry run python examples/indexing_pipeline_example.py

# Query indexed documents
poetry run python examples/retrieval_pipeline_example.py

# Start MCP server (stdio transport)
poetry run python examples/mcp_server_example.py
```

## Examples

### `indexing_pipeline_example.py`

Creates two indexing strategies in the same project:
- **Fast**: 300 char chunks, all-MiniLM-L6-v2
- **Semantic**: 800 char chunks, all-mpnet-base-v2
- Uses `project="demo_rag_app"` for organization

### `retrieval_pipeline_example.py`

Multi-source retrieval with evaluation:
- Queries both indexing pipelines from the same project
- Re-ranks results using cross-encoder
- Generates answers via OpenRouter
- Evaluates with BLEU, ROUGE, coherence, readability
- **Important**: Must use same `project` as indexing pipelines

### `mcp_server_example.py`

Runs an MCP server that exposes `agentic-rag` operations as tools:
- create pipelines (indexing and retrieval)
- load pipelines for execution
- run indexing and retrieval pipelines
- list projects, pipelines, and components

## Storage & Organization

- **Neo4j**: Pipeline graphs with `User → Project → Pipelines` hierarchy
- **ChromaDB**: Vector embeddings at `./data/{username}/{project}/{pipeline_name}/`
- **Akave**: Document content on decentralized S3-compatible storage with user-based access control
