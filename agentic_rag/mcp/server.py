"""MCP server exposing agentic-rag pipeline operations as tools."""

from __future__ import annotations

import contextlib
import sys
from dataclasses import asdict, is_dataclass
from typing import Any, Callable, Dict, List, Optional, TypeVar

from agentic_rag import Config, PipelineFactory
from agentic_rag.components import get_default_registry
from agentic_rag.pipeline import PipelineRunner
from agentic_rag.types import (
    PipelineSpec,
    get_component_value,
    list_available_components,
)
from agentic_rag.types.component_enums import get_evaluator_mode, requires_gold_standard

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover - import-time dependency guard
    raise ImportError(
        "MCP dependencies are missing. Install with `pip install mcp` "
        "or use `poetry install` after updating dependencies."
    ) from exc


EMBEDDING_FIELD_NAMES = {
    "embedding",
    "embeddings",
    "query_embedding",
    "sparse_embedding",
    "sparse_embeddings",
}

T = TypeVar("T")


def _to_jsonable(value: Any, depth: int = 0, max_depth: int = 5) -> Any:
    """Convert nested objects to JSON-friendly data."""
    if depth > max_depth:
        return repr(value)

    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if is_dataclass(value):
        return _to_jsonable(asdict(value), depth + 1, max_depth)

    if isinstance(value, dict):
        return {
            str(key): _to_jsonable(item, depth + 1, max_depth)
            for key, item in value.items()
            if str(key).lower() not in EMBEDDING_FIELD_NAMES
        }

    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(item, depth + 1, max_depth) for item in value]

    if hasattr(value, "to_dict") and callable(value.to_dict):
        try:
            return _to_jsonable(value.to_dict(), depth + 1, max_depth)
        except Exception:
            return repr(value)

    if hasattr(value, "__dict__"):
        raw = vars(value)
        safe = {
            key: _to_jsonable(item, depth + 1, max_depth)
            for key, item in raw.items()
            if not key.startswith("_") and key.lower() not in EMBEDDING_FIELD_NAMES
        }
        if safe:
            return safe

    return repr(value)


def _run_with_stdout_redirect(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """
    Run tool logic while redirecting accidental stdout to stderr.

    MCP stdio transport requires stdout to contain only JSON-RPC payloads.
    """
    with contextlib.redirect_stdout(sys.stderr):
        return func(*args, **kwargs)


def _extract_replies(component_result: Dict[str, Any]) -> List[str]:
    """Extract text replies from a generator-like component output."""
    replies = component_result.get("replies")
    if isinstance(replies, list):
        return [str(reply) for reply in replies]
    if isinstance(replies, str):
        return [replies]
    return []


def _extract_eval_metrics(component_result: Dict[str, Any]) -> Dict[str, Any]:
    """Extract evaluation metrics from evaluator output."""
    eval_data = component_result.get("eval_data")
    if isinstance(eval_data, dict):
        metrics = eval_data.get("eval_metrics")
        if isinstance(metrics, dict):
            return metrics
    return {}


def _summarize_retrieval_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """Return a compact retrieval summary with answer and metric results."""
    branch_summaries: Dict[str, Any] = {}
    first_answer: Optional[str] = None

    branches = result.get("branches", {})
    if not isinstance(branches, dict):
        return {"query": result.get("query"), "branches": {}}

    for branch_id, branch_result in branches.items():
        if isinstance(branch_result, dict) and "error" in branch_result:
            branch_summaries[branch_id] = {"error": branch_result["error"]}
            continue

        branch_answer: Optional[str] = None
        branch_metrics: Dict[str, Any] = {}
        document_count = 0

        if isinstance(branch_result, dict):
            for component_output in branch_result.values():
                if not isinstance(component_output, dict):
                    continue

                replies = _extract_replies(component_output)
                if replies and branch_answer is None:
                    branch_answer = replies[0]

                metrics = _extract_eval_metrics(component_output)
                if metrics:
                    branch_metrics.update(metrics)

                documents = component_output.get("documents")
                if isinstance(documents, list):
                    document_count = max(document_count, len(documents))

        if first_answer is None and branch_answer:
            first_answer = branch_answer

        branch_summaries[branch_id] = {
            "answer": branch_answer,
            "metrics": branch_metrics,
            "document_count": document_count,
        }

    return {
        "query": result.get("query"),
        "answer": first_answer,
        "branches": branch_summaries,
        "branches_count": result.get("branches_count", len(branch_summaries)),
        "total_documents": result.get("total_documents"),
    }


class PipelineMCPService:
    """Thin service layer wrapping Factory/Runner/GraphStore operations."""

    def __init__(self, config: Optional[Config] = None) -> None:
        self.config = config or Config()
        self.registry = get_default_registry()
        self._factory: Optional[PipelineFactory] = None
        self._runner: Optional[PipelineRunner] = None

    @property
    def factory(self) -> PipelineFactory:
        """Lazily create the pipeline factory."""
        if self._factory is None:
            self._factory = PipelineFactory(config=self.config)
        return self._factory

    @property
    def runner(self) -> PipelineRunner:
        """Lazily create the pipeline runner."""
        if self._runner is None:
            self._runner = PipelineRunner(config=self.config, enable_caching=False)
        return self._runner

    @property
    def graph_store(self) -> Any:
        """Access graph store via runner to avoid eager Neo4j connection on startup."""
        return self.runner.graph_store

    def list_projects_and_pipelines(self, username: str) -> List[Dict[str, Any]]:
        """List all projects and pipeline names for a user."""
        with self.graph_store.driver.session(
            database=self.graph_store.database
        ) as session:
            query = """
                MATCH (u:User {username: $username})-[:OWNS]->(p:Project)
                OPTIONAL MATCH (p)-[:FLOWS_TO]->(c:Component)
                WHERE c.pipeline_name IS NOT NULL
                WITH p, c.pipeline_name as pname, c.pipeline_type as ptype
                WITH p, collect(DISTINCT CASE WHEN pname IS NOT NULL THEN {name: pname, type: ptype} END) as raw_pipelines
                WITH p, [x IN raw_pipelines WHERE x IS NOT NULL] as pipelines
                RETURN {
                    project: p.name,
                    pipelines: [x in pipelines | x.name],
                    details: pipelines
                } as project_data
            """
            results = session.run(query, username=username).data()
            return [record["project_data"] for record in results]

    def get_component_index(self) -> Dict[str, Any]:
        """
        Return a full component index linking enum specs to concrete registry entries.

        Useful for agents to discover exact component specs they can compose in
        `create_pipelines` calls.
        """
        available_specs = list_available_components()
        entries: List[Dict[str, Any]] = []

        for category, member_names in available_specs.items():
            for member_name in member_names:
                spec_string = f"{category}.{member_name}"
                component_name = get_component_value(spec_string)
                registered_spec = self.registry.get_component_spec(component_name)

                entry: Dict[str, Any] = {
                    "spec": spec_string,
                    "category": category,
                    "member": member_name,
                    "component_name": component_name,
                    "registered": registered_spec is not None,
                }

                if registered_spec is not None:
                    entry.update(
                        {
                            "component_type": registered_spec.component_type.value,
                            "pipeline_usage": registered_spec.pipeline_usage.value,
                            "haystack_class": registered_spec.haystack_class,
                            "default_config_keys": sorted(
                                list(registered_spec.default_config.keys())
                            ),
                        }
                    )
                    if category == "EVALUATOR":
                        entry.update(
                            {
                                "requires_ground_truth": requires_gold_standard(
                                    component_name
                                ),
                                "evaluation_mode": get_evaluator_mode(
                                    component_name
                                ).value,
                            }
                        )

                entries.append(entry)

        return {
            "total_specs": len(entries),
            "total_registered_components": len(self.registry.list_components()),
            "component_specs": entries,
            "registered_components": sorted(self.registry.list_components()),
        }

    def create_pipelines(
        self,
        username: str,
        project: str,
        pipeline_specs: List[List[Dict[str, str]]],
        configs: Optional[List[Dict[str, Any]]] = None,
        pipeline_types: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Create one or more pipeline graphs in Neo4j."""
        created: List[PipelineSpec] = self.factory.build_pipeline_graphs_from_specs(
            pipeline_specs=pipeline_specs,
            username=username,
            project=project,
            configs=configs,
            pipeline_types=pipeline_types,
        )
        return {
            "count": len(created),
            "pipelines": [
                {
                    "name": pipe.name,
                    "pipeline_type": pipe.pipeline_type.value,
                    "component_count": len(pipe.components),
                    "components": [comp.name for comp in pipe.components],
                    "indexing_pipelines": pipe.indexing_pipelines,
                }
                for pipe in created
            ],
        }

    def load_pipelines(
        self, username: str, project: str, pipeline_names: List[str]
    ) -> Dict[str, Any]:
        """Load one or more pipelines for execution."""
        self.runner.load_pipelines(
            pipeline_names=pipeline_names,
            username=username,
            project=project,
        )
        return {
            "loaded": pipeline_names,
            "loaded_count": len(pipeline_names),
        }

    def run_pipeline(
        self,
        username: str,
        project: str,
        pipeline_name: str,
        pipeline_type: str,
        inputs: Dict[str, Any],
        auto_load: bool = True,
    ) -> Dict[str, Any]:
        """Run a pipeline with provided input payload."""
        if auto_load:
            self.runner.load_pipelines(
                pipeline_names=[pipeline_name],
                username=username,
                project=project,
            )

        result = self.runner.run(
            pipeline_name=pipeline_name,
            username=username,
            type=pipeline_type,
            project=project,
            **inputs,
        )

        summarized_result: Any = result
        if pipeline_type == "retrieval" and isinstance(result, dict):
            summarized_result = _summarize_retrieval_result(result)

        return {
            "pipeline_name": pipeline_name,
            "pipeline_type": pipeline_type,
            "project": project,
            "result": _to_jsonable(summarized_result),
        }

    def list_pipeline_components(
        self, username: str, project: str, pipeline_name: str
    ) -> Dict[str, Any]:
        """List persisted component nodes for a pipeline."""
        components = self.graph_store.get_components_by_pipeline(
            pipeline_name=pipeline_name,
            username=username,
            project=project,
        )
        return {
            "pipeline_name": pipeline_name,
            "component_count": len(components),
            "components": _to_jsonable(components),
        }


def create_mcp_server(config: Optional[Config] = None) -> FastMCP:
    """Create and configure the MCP server instance."""
    mcp = FastMCP("agentic-rag")
    service = PipelineMCPService(config=config)

    @mcp.tool()  # type: ignore[misc]
    def health() -> Dict[str, Any]:
        """Simple connectivity and config status check."""
        return _run_with_stdout_redirect(
            lambda: {
                "service": "agentic-rag-mcp",
                "neo4j_configured": service.config.validate_neo4j(),
                "openrouter_configured": service.config.validate_openrouter(),
            }
        )

    @mcp.tool()  # type: ignore[misc]
    def list_available_component_specs() -> Dict[str, List[str]]:
        """List component spec enums grouped by category."""
        return _run_with_stdout_redirect(list_available_components)

    @mcp.tool()  # type: ignore[misc]
    def list_registered_components() -> List[str]:
        """List concrete registered component names from registry."""
        return _run_with_stdout_redirect(service.registry.list_components)

    @mcp.tool()  # type: ignore[misc]
    def get_component_index() -> Dict[str, Any]:
        """
        Return exact component index for MCP agents.

        Includes:
        - enum spec strings like `CONVERTER.PDF`
        - concrete registry names like `pdf_converter`
        - registration status and metadata

        Example call:
        {
          "tool": "get_component_index",
          "arguments": {}
        }
        """
        return _run_with_stdout_redirect(service.get_component_index)

    @mcp.tool()  # type: ignore[misc]
    def list_projects(username: str) -> List[Dict[str, Any]]:
        """
        List projects and pipelines available for a username.

        Example call:
        {
          "tool": "list_projects",
          "arguments": {
            "username": "alice"
          }
        }
        """
        return _run_with_stdout_redirect(
            service.list_projects_and_pipelines,
            username=username,
        )

    @mcp.tool()  # type: ignore[misc]
    def list_pipeline_components(
        username: str,
        project: str,
        pipeline_name: str,
    ) -> Dict[str, Any]:
        """
        List components in a specific pipeline.

        Example call:
        {
          "tool": "list_pipeline_components",
          "arguments": {
            "username": "alice",
            "project": "demo_rag_app",
            "pipeline_name": "paper_index"
          }
        }
        """
        return _run_with_stdout_redirect(
            service.list_pipeline_components,
            username=username,
            project=project,
            pipeline_name=pipeline_name,
        )

    @mcp.tool()  # type: ignore[misc]
    def create_pipelines(
        username: str,
        project: str,
        pipeline_specs: List[List[Dict[str, str]]],
        configs: Optional[List[Dict[str, Any]]] = None,
        pipeline_types: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Create pipeline graphs.

        - `pipeline_specs` is a list of pipelines.
        - Each pipeline is a list of component dicts with `type`.
        - Example component: `{"type": "CONVERTER.PDF"}`.
        - `pipeline_types` must contain one string per pipeline, usually
          `"indexing"` or `"retrieval"`.
        - For indexing pipelines, put component specs inside `pipeline_specs`,
          not inside `pipeline_types`.

        Example indexing pipeline call:
        {
          "tool": "create_pipelines",
          "arguments": {
            "username": "alice",
            "project": "demo_rag_app",
            "pipeline_specs": [[
              {"type": "CONVERTER.MARKDOWN"},
              {"type": "CHUNKER.MARKDOWN_AWARE"},
              {"type": "EMBEDDER.SENTENCE_TRANSFORMERS_DOC"},
              {"type": "WRITER.CHROMA_DOCUMENT_WRITER"}
            ]],
            "configs": [{
              "_pipeline_name": "paper_index",
              "markdown_aware_chunker": {
                "chunk_size": 800,
                "chunk_overlap": 100
              }
            }],
            "pipeline_types": ["indexing"]
          }
        }

        Example retrieval pipeline call:
        {
          "tool": "create_pipelines",
          "arguments": {
            "username": "alice",
            "project": "demo_rag_app",
            "pipeline_specs": [[
              {"type": "INDEX"},
              {"type": "GENERATOR.PROMPT_BUILDER"},
              {"type": "GENERATOR.OPENROUTER"}
            ]],
            "configs": [{
              "_pipeline_name": "paper_retrieval",
              "_indexing_pipelines": ["paper_index"],
              "prompt_builder": {
                "template": "Answer the question based only on the provided documents.\n\nQuestion: {{query}}\n\nDocuments:\n{% for doc in documents %}\n{{ doc.content }}\n---\n{% endfor %}\n\nAnswer:"
              }
            }],
            "pipeline_types": ["retrieval"]
          }
        }
        """
        return _run_with_stdout_redirect(
            service.create_pipelines,
            username=username,
            project=project,
            pipeline_specs=pipeline_specs,
            configs=configs,
            pipeline_types=pipeline_types,
        )

    @mcp.tool()  # type: ignore[misc]
    def load_pipelines(
        username: str,
        project: str,
        pipeline_names: List[str],
    ) -> Dict[str, Any]:
        """
        Load one or more pipeline graphs into the runtime runner.

        Example call:
        {
          "tool": "load_pipelines",
          "arguments": {
            "username": "alice",
            "project": "demo_rag_app",
            "pipeline_names": ["paper_index", "paper_retrieval"]
          }
        }
        """
        return _run_with_stdout_redirect(
            service.load_pipelines,
            username=username,
            project=project,
            pipeline_names=pipeline_names,
        )

    @mcp.tool()  # type: ignore[misc]
    def run_indexing_pipeline(
        username: str,
        project: str,
        pipeline_name: str,
        data_path: str,
        auto_load: bool = True,
    ) -> Dict[str, Any]:
        """
        Run an indexing pipeline. Input requires `data_path`.

        Example call:
        {
          "tool": "run_indexing_pipeline",
          "arguments": {
            "username": "alice",
            "project": "demo_rag_app",
            "pipeline_name": "paper_index",
            "data_path": "/absolute/path/to/papers",
            "auto_load": true
          }
        }
        """
        return _run_with_stdout_redirect(
            service.run_pipeline,
            username=username,
            project=project,
            pipeline_name=pipeline_name,
            pipeline_type="indexing",
            inputs={"data_path": data_path},
            auto_load=auto_load,
        )

    @mcp.tool()  # type: ignore[misc]
    def run_retrieval_pipeline(
        username: str,
        project: str,
        pipeline_name: str,
        query: str,
        ground_truth_answer: Optional[str] = None,
        relevant_doc_ids: Optional[List[str]] = None,
        auto_load: bool = True,
    ) -> Dict[str, Any]:
        """
        Run a retrieval pipeline with a query.

        Example call:
        {
          "tool": "run_retrieval_pipeline",
          "arguments": {
            "username": "alice",
            "project": "demo_rag_app",
            "pipeline_name": "paper_retrieval",
            "query": "What chunking strategy is recommended?",
            "auto_load": true
          }
        }
        """
        return _run_with_stdout_redirect(
            service.run_pipeline,
            username=username,
            project=project,
            pipeline_name=pipeline_name,
            pipeline_type="retrieval",
            inputs={
                "query": query,
                "ground_truth_answer": ground_truth_answer,
                "relevant_doc_ids": relevant_doc_ids or [],
            },
            auto_load=auto_load,
        )

    @mcp.tool()  # type: ignore[misc]
    def run_pipeline(
        username: str,
        project: str,
        pipeline_name: str,
        pipeline_type: str,
        inputs: Dict[str, Any],
        auto_load: bool = True,
    ) -> Dict[str, Any]:
        """
        Generic execution tool for indexing or retrieval pipelines.

        Example indexing call:
        {
          "tool": "run_pipeline",
          "arguments": {
            "username": "alice",
            "project": "demo_rag_app",
            "pipeline_name": "paper_index",
            "pipeline_type": "indexing",
            "inputs": {
              "data_path": "/absolute/path/to/papers"
            },
            "auto_load": true
          }
        }

        Example retrieval call:
        {
          "tool": "run_pipeline",
          "arguments": {
            "username": "alice",
            "project": "demo_rag_app",
            "pipeline_name": "paper_retrieval",
            "pipeline_type": "retrieval",
            "inputs": {
              "query": "Summarize the paper's main contribution"
            },
            "auto_load": true
          }
        }
        """
        return _run_with_stdout_redirect(
            service.run_pipeline,
            username=username,
            project=project,
            pipeline_name=pipeline_name,
            pipeline_type=pipeline_type,
            inputs=inputs,
            auto_load=auto_load,
        )

    return mcp


def main() -> None:
    """Run MCP server over stdio transport."""
    server = create_mcp_server()
    server.run()


if __name__ == "__main__":
    main()
