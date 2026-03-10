"""Pipeline runner for executing Haystack pipelines with data.

ARCHITECTURE:
- Factory: Build pipelines once and store in Neo4j (creation time)
- Runner: Load pipelines from Neo4j and execute (runtime)

FAIR Compliance:
- Creates RunNode for each pipeline execution (provenance)
- Links all generated DataPieces to the Run via GENERATED_BY
- Stores run metadata: pipeline_name, started_at, finished_at, success
"""

import hashlib
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..components import GraphStore
from ..config import Config, get_config
from ..types import PipelineUsage
from ..types.node_types import ARKHAI_NAMESPACE
from ..utils.logger import configure_haystack_logging, get_logger
from ..utils.metrics import MetricsCollector


def generate_run_id() -> str:
    """Generate a unique run ID."""
    unique = f"{datetime.utcnow().isoformat()}_{uuid.uuid4().hex[:8]}"
    hash_obj = hashlib.sha256(unique.encode())
    return f"run_{hash_obj.hexdigest()[:12]}"


class PipelineRunner:
    """Executes pipelines with input data (Singleton)."""

    _instance: Optional["PipelineRunner"] = None
    _initialized: bool = False

    def __new__(cls, *args: Any, **kwargs: Any) -> "PipelineRunner":
        """Ensure only one instance of PipelineRunner exists."""
        if cls._instance is None:
            cls._instance = super(PipelineRunner, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        graph_store: Optional[GraphStore] = None,
        enable_caching: bool = False,
        config: Optional[Config] = None,
    ) -> None:
        """
        Initialize the pipeline runner (singleton).

        Args:
            graph_store: GraphStore for loading pipelines from Neo4j (created from config if not provided)
            enable_caching: If True, wraps components with GatedComponent for caching
            config: Configuration object with API keys and credentials for components

        Note:
            This is a singleton class. Only the first initialization will be used.
            Username and pipeline names are now injected at method level.
        """
        # Only initialize once
        if self._initialized:
            return

        self.enable_caching = enable_caching
        self.config = get_config(
            config
        )  # Use provided config or fallback to global/env

        # Create GraphStore from config if not provided
        if graph_store is None:
            self.graph_store = GraphStore(config=self.config)
        else:
            self.graph_store = graph_store

        self.logger = get_logger(__name__, config=self.config)

        # Configure Haystack logging
        configure_haystack_logging(level="DEBUG")

        # Graph representations from Neo4j for multiple pipelines
        self._pipeline_graphs: Dict[str, List[Dict[str, Any]]] = {}
        # Haystack components by pipeline (not yet connected)
        self._haystack_components_by_pipeline: Dict[str, Dict[str, Any]] = {}
        # Actual Haystack Pipeline objects (connected and ready to run)
        self._haystack_pipelines: Dict[str, Any] = {}

        self._initialized = True
        self.logger.info("PipelineRunner initialized (singleton)")

    def _set_run_id_on_components(self, pipeline_name: str, run_id: str) -> None:
        """
        Set run_id on all GatedComponents in a pipeline for FAIR provenance.

        Args:
            pipeline_name: Name of the pipeline
            run_id: Run ID to set on all components
        """
        from ..components.gates import GatedComponent

        if pipeline_name not in self._haystack_components_by_pipeline:
            return

        components = self._haystack_components_by_pipeline[pipeline_name]
        for comp_id, component in components.items():
            if isinstance(component, GatedComponent):
                component.set_run_id(run_id)
                self.logger.debug(f"Set run_id={run_id} on component {comp_id}")

    def _create_run_node(
        self,
        run_id: str,
        pipeline_name: str,
        username: str,
        project: str,
        pipeline_type: str,
    ) -> None:
        """
        Create a RunNode in Neo4j for provenance tracking.

        Args:
            run_id: Unique run identifier
            pipeline_name: Name of pipeline being run
            username: User who initiated the run
            project: Project name
            pipeline_type: "indexing" or "retrieval"
        """
        started_at = datetime.utcnow().isoformat()
        uri = f"{ARKHAI_NAMESPACE}/run/{run_id}"

        self.graph_store.store_run_node(
            run_id=run_id,
            pipeline_name=pipeline_name,
            username=username,
            project=project,
            started_at=started_at,
            uri=uri,
        )
        self.logger.info(f"Created RunNode: {run_id} for pipeline {pipeline_name}")

    async def _create_run_node_async(
        self,
        run_id: str,
        pipeline_name: str,
        username: str,
        project: str,
        pipeline_type: str,
    ) -> None:
        """Async version of _create_run_node."""
        started_at = datetime.utcnow().isoformat()
        uri = f"{ARKHAI_NAMESPACE}/run/{run_id}"

        await self.graph_store.store_run_node_async(
            run_id=run_id,
            pipeline_name=pipeline_name,
            username=username,
            project=project,
            started_at=started_at,
            uri=uri,
        )
        self.logger.info(
            f"Created RunNode (async): {run_id} for pipeline {pipeline_name}"
        )

    def _finalize_run_node(
        self, run_id: str, success: bool, error: Optional[str] = None
    ) -> None:
        """Update RunNode with completion information."""
        finished_at = datetime.utcnow().isoformat()
        self.graph_store.update_run_finished(
            run_id=run_id,
            finished_at=finished_at,
            success=success,
            error=error,
        )
        self.logger.info(f"Finalized RunNode: {run_id} (success={success})")

    async def _finalize_run_node_async(
        self, run_id: str, success: bool, error: Optional[str] = None
    ) -> None:
        """Async version of _finalize_run_node."""
        finished_at = datetime.utcnow().isoformat()
        await self.graph_store.update_run_finished_async(
            run_id=run_id,
            finished_at=finished_at,
            success=success,
            error=error,
        )
        self.logger.info(f"Finalized RunNode (async): {run_id} (success={success})")

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton instance (useful for testing)."""
        cls._instance = None
        cls._initialized = False

    def load_pipelines(
        self, pipeline_names: List[str], username: str, project: str = "default"
    ) -> None:
        """
        Load and build pipelines for a specific user.

        Args:
            pipeline_names: List of pipeline names to load.
            username: Username to load pipelines for
            project: Project name (defaults to "default")
        """
        self.logger.info(
            f"Loading pipelines for user {username}, project {project}: {pipeline_names}"
        )

        for pipeline_name in pipeline_names:
            try:
                # Load pipeline graph
                self.logger.debug(f"Loading pipeline graph: {pipeline_name}")
                self.load_pipeline_graph([pipeline_name], username, project)

                # Build Haystack components
                self.logger.debug(f"Building Haystack components: {pipeline_name}")
                self.build_haystack_components_from_graph(pipeline_name, username)

                # Get pipeline type from Neo4j (stored in component nodes)
                pipeline_type = self._get_pipeline_type(pipeline_name)

                # Create connected pipeline
                self.logger.debug(
                    f"Creating connected {pipeline_type} pipeline: {pipeline_name}"
                )
                self.create_haystack_pipeline(
                    pipeline_name, username, pipeline_type=pipeline_type
                )

                self.logger.info(f"Successfully loaded pipeline: {pipeline_name}")

            except Exception as e:
                self.logger.warning(f"Could not load {pipeline_name}: {e}")

        self.logger.info(f"Total pipelines loaded: {len(self._haystack_pipelines)}")

    async def load_pipelines_async(
        self, pipeline_names: List[str], username: str, project: str = "default"
    ) -> None:
        """
        Async version of load_pipelines.

        Load and build pipelines for a specific user.

        Args:
            pipeline_names: List of pipeline names to load.
            username: Username to load pipelines for
            project: Project name (defaults to "default")
        """
        self.logger.info(
            f"Loading pipelines (async) for user {username}, project {project}: {pipeline_names}"
        )

        for pipeline_name in pipeline_names:
            try:
                # Load pipeline graph (async)
                self.logger.debug(f"Loading pipeline graph (async): {pipeline_name}")
                await self.load_pipeline_graph_async([pipeline_name], username, project)

                # Build Haystack components
                # Note: Building components is still sync as it just instantiates Python objects
                self.logger.debug(f"Building Haystack components: {pipeline_name}")
                self.build_haystack_components_from_graph(pipeline_name, username)

                # Get pipeline type from Neo4j (stored in component nodes)
                pipeline_type = self._get_pipeline_type(pipeline_name)

                # Create connected pipeline
                self.logger.debug(
                    f"Creating connected {pipeline_type} pipeline: {pipeline_name}"
                )
                await self.create_haystack_pipeline_async(
                    pipeline_name, username, pipeline_type=pipeline_type
                )

                self.logger.info(
                    f"Successfully loaded pipeline (async): {pipeline_name}"
                )

            except Exception as e:
                self.logger.warning(f"Could not load {pipeline_name}: {e}")

        self.logger.info(
            f"Total pipelines loaded (async): {len(self._haystack_pipelines)}"
        )

    def _get_pipeline_type(self, pipeline_name: str) -> str:
        """
        Get the pipeline type from loaded component metadata.

        Args:
            pipeline_name: Name of the pipeline

        Returns:
            str: "indexing" or "retrieval"

        Raises:
            ValueError: If pipeline type cannot be determined
        """
        if pipeline_name not in self._pipeline_graphs:
            raise ValueError(f"Pipeline {pipeline_name} not loaded")

        components = self._pipeline_graphs[pipeline_name]
        if not components:
            raise ValueError(f"No components found for pipeline {pipeline_name}")

        # Get pipeline_type from first component (all components have same pipeline_type)
        pipeline_type = components[0].get("pipeline_type")

        if not pipeline_type:
            # Fallback to old auto-detection if pipeline_type not stored
            self.logger.warning(
                f"Pipeline type not found in Neo4j for {pipeline_name}, "
                "using fallback detection"
            )
            pipeline_type = (
                "retrieval" if "retrieval" in pipeline_name.lower() else "indexing"
            )

        return pipeline_type

    def load_pipeline_graph(
        self, pipeline_hashes: List[str], username: str, project: str = "default"
    ) -> None:
        """
        Load pipeline metadata from Neo4j and store in _pipeline_graphs.

        Fetches all component metadata including configuration, connections,
        and branch information for the specified pipelines.

        Args:
            pipeline_hashes: List of pipeline names to load (e.g., ['retrieval_pipeline'])
            username: Username for permissions
            project: Project name to filter by (defaults to "default")

        Raises:
            RuntimeError: If no graph store is configured
            ValueError: If pipeline not found

        Updates:
            self._pipeline_graphs: Dict mapping pipeline names to component metadata

        Example for Indexing Pipeline:
            Input:
                pipeline_hashes=['pipeline_small']

            Output stored in _pipeline_graphs:
                {
                    'pipeline_small': [
                        {
                            'id': 'comp_123',
                            'component_name': 'markdown_aware_chunker',
                            'component_type': 'CHUNKER.MARKDOWN_AWARE',
                            'component_config_json': '{"chunk_size": 500}',
                            'pipeline_name': 'pipeline_small',
                            'next_components': ['comp_456'],
                            'cache_key': 'cache_abc123',
                            'node_labels': ['Component']
                        },
                        # ... more components
                    ]
                }

        Example for Retrieval Pipeline (with branches):
            Input:
                pipeline_hashes=['retrieval_pipeline']

            Output stored in _pipeline_graphs:
                {
                    'retrieval_pipeline': [
                        # Branch 1 components
                        {
                            'id': 'comp_789',
                            'component_name': 'embedder',
                            'branch_id': 'pipeline_small',  # ← Branch identifier
                            'pipeline_name': 'retrieval_pipeline',
                            'next_components': ['comp_890'],
                            ...
                        },
                        # Branch 2 components
                        {
                            'id': 'comp_abc',
                            'component_name': 'embedder',
                            'branch_id': 'pipeline_medium',  # ← Different branch
                            'pipeline_name': 'retrieval_pipeline',
                            ...
                        }
                    ]
                }
        """
        if not self.graph_store:
            raise RuntimeError(
                "No graph store configured. Pass GraphStore to constructor."
            )

        # Delegate to GraphStorage for actual loading logic
        from ..components import get_default_registry
        from .storage import GraphStorage

        registry = get_default_registry()
        graph_storage = GraphStorage(self.graph_store, registry)

        pipelines_data = graph_storage.load_pipeline_by_hashes(
            pipeline_hashes, username, project
        )

        # Store the graph representations for all loaded pipelines
        self._pipeline_graphs.update(pipelines_data)

    async def load_pipeline_graph_async(
        self, pipeline_hashes: List[str], username: str, project: str = "default"
    ) -> None:
        """
        Async version of load_pipeline_graph.

        Load pipeline metadata from Neo4j and store in _pipeline_graphs.

        Args:
            pipeline_hashes: List of pipeline names to load
            username: Username for permissions
            project: Project name to filter by (defaults to "default")
        """
        if not self.graph_store:
            raise RuntimeError(
                "No graph store configured. Pass GraphStore to constructor."
            )

        # Delegate to GraphStorage for actual loading logic
        from ..components import get_default_registry
        from .storage import GraphStorage

        registry = get_default_registry()
        graph_storage = GraphStorage(self.graph_store, registry)

        pipelines_data = await graph_storage.load_pipeline_by_hashes_async(
            pipeline_hashes, username, project
        )

        # Store the graph representations for all loaded pipelines
        self._pipeline_graphs.update(pipelines_data)

    def build_haystack_components_from_graph(
        self, pipeline_name: str, username: str
    ) -> None:
        """
        Instantiate Haystack component objects from metadata and store in _haystack_components_by_pipeline.

        Takes component metadata from _pipeline_graphs, creates actual Python objects
        (e.g., SentenceTransformersTextEmbedder, ChromaEmbeddingRetriever), and optionally
        wraps them with GatedComponent for caching.

        Args:
            pipeline_name: Name of the pipeline to build components for

        Raises:
            RuntimeError: If no pipeline graph is loaded
            ValueError: If pipeline name not found in loaded data

        Updates:
            self._haystack_components_by_pipeline: Dict mapping pipeline names to component objects

        Example Input (from _pipeline_graphs):
            _pipeline_graphs = {
                'retrieval_pipeline': [
                    {
                        'id': 'comp_789',
                        'component_name': 'embedder',
                        'component_config_json': '{"model": "all-MiniLM-L6-v2"}',
                        'cache_key': 'cache_abc123',
                        'branch_id': 'pipeline_small'
                    },
                    {
                        'id': 'comp_890',
                        'component_name': 'chroma_embedding_retriever',
                        'component_config_json': '{"root_dir": "./data/...", "top_k": 5}',
                        'cache_key': 'cache_def456',
                        'branch_id': 'pipeline_small'
                    }
                ]
            }

        Example Output (stored in _haystack_components_by_pipeline):
            _haystack_components_by_pipeline = {
                'retrieval_pipeline': {
                    'comp_789': <SentenceTransformersTextEmbedder(model='all-MiniLM-L6-v2')>,
                    'comp_890': <ChromaEmbeddingRetriever(root_dir='./data/...', top_k=5)>
                }
            }

        Note:
            - All branch components are stored together (no separation by branch_id yet)
            - Actual component objects are created, configured, and ready to use
            - If enable_caching=True, components are wrapped with GatedComponent
            - The branch_id stays in metadata, not in the component objects
        """
        if not self._pipeline_graphs:
            raise RuntimeError(
                "No pipeline graphs loaded. Call load_pipeline_graph() first."
            )

        if pipeline_name not in self._pipeline_graphs:
            raise ValueError(
                f"Pipeline '{pipeline_name}' not found in loaded data. "
                f"Available: {list(self._pipeline_graphs.keys())}"
            )

        components_data = self._pipeline_graphs[pipeline_name]

        # Import registry to look up component specs
        from ..components import get_default_registry

        registry = get_default_registry()

        # Build components dynamically
        haystack_components = {}

        self.logger.debug(f"Building Haystack components for: {pipeline_name}")

        for comp_data in components_data:
            comp_id = comp_data.get("id")
            cache_key = comp_data.get("cache_key")  # Pipeline-agnostic cache key
            node_labels = comp_data.get("node_labels", [])

            # Skip DocumentStore nodes - they're created automatically by components
            if "DocumentStore" in node_labels:
                continue

            # Handle Component nodes
            comp_name = comp_data.get("component_name")
            config_json = comp_data.get("component_config_json", "{}")

            if not comp_name or not comp_id:
                continue

            try:
                # Get the base component spec from registry
                base_spec = registry.get_component_spec(comp_name)
                if not base_spec:
                    self.logger.error(f"Component '{comp_name}' not found in registry")
                    continue

                # Parse and apply configuration
                import json
                from copy import deepcopy

                component_config = (
                    json.loads(config_json) if config_json != "{}" else {}
                )

                # Create a copy to avoid mutating the registry's spec
                spec_copy = deepcopy(base_spec)

                # Inject Config object for components that need API keys
                # (OpenRouterGenerator, evaluators that use LLMs)
                needs_config = any(
                    [
                        "openrouter" in comp_name.lower(),
                        "answer_quality" in comp_name.lower(),
                        "answer_structure" in comp_name.lower(),
                        "communication_quality" in comp_name.lower(),
                        "fact_matching" in comp_name.lower(),
                        "longqa" in comp_name.lower(),
                        "morqa" in comp_name.lower(),
                    ]
                )

                if needs_config and "config" not in component_config:
                    component_config["config"] = self.config

                # Configure with loaded config
                if component_config:
                    configured_spec = spec_copy.configure(component_config)
                else:
                    configured_spec = spec_copy

                # Instantiate a FRESH Haystack component (no caching)
                # Haystack doesn't allow sharing component instances between pipelines
                from ..components.registry import create_haystack_component

                haystack_component = create_haystack_component(configured_spec)

                # Optionally wrap with GatedComponent for caching
                if self.enable_caching:
                    # Skip wrapping document stores and writers (they're already persistent)
                    skip_wrapping = (
                        "writer" in comp_name.lower() or "store" in comp_name.lower()
                    )

                    if not skip_wrapping:
                        from ..components.gates import GatedComponent

                        self.logger.info(f"Wrapping {comp_name} with caching gates")
                        haystack_component = GatedComponent(
                            component=haystack_component,
                            component_id=comp_id,
                            component_name=comp_name,
                            graph_store=self.graph_store,
                            username=username,
                            cache_key=cache_key,  # Use pipeline-agnostic cache key
                            retrieve_from_storage=True,
                        )
                    else:
                        self.logger.debug(
                            f"Skipping gate wrapping for {comp_name} (writer/store)"
                        )

                haystack_components[comp_id] = haystack_component

                self.logger.debug(
                    f"Built {comp_name} ({type(haystack_component).__name__})"
                )

            except Exception as e:
                self.logger.error(f"Error building {comp_name}: {e}", exc_info=True)

        self.logger.info(f"Built {len(haystack_components)} components")

        # Store the components for this pipeline
        self._haystack_components_by_pipeline[pipeline_name] = haystack_components

    def create_haystack_pipeline(
        self, pipeline_name: str, username: str, pipeline_type: str = "indexing"
    ) -> Any:
        """
        Create Haystack Pipeline(s) by routing to appropriate builder.

        Args:
            pipeline_name: Name of the pipeline to create
            pipeline_type: Type of pipeline - "indexing" or "retrieval" (default: "indexing")

        Returns:
            Connected Haystack Pipeline object(s) ready to run

        Raises:
            RuntimeError: If components haven't been built yet
        """
        if pipeline_type == "indexing":
            return self.create_haystack_pipeline_indexing(pipeline_name, username)
        elif pipeline_type == "retrieval":
            return self.create_haystack_pipeline_retrieval(pipeline_name, username)
        else:
            raise ValueError(
                f"Invalid pipeline_type: {pipeline_type}. Must be 'indexing' or 'retrieval'"
            )

    async def create_haystack_pipeline_async(
        self, pipeline_name: str, username: str, pipeline_type: str = "indexing"
    ) -> Any:
        """
        Async version of create_haystack_pipeline.
        """
        if pipeline_type == "indexing":
            return await self.create_haystack_pipeline_indexing_async(
                pipeline_name, username
            )
        elif pipeline_type == "retrieval":
            return await self.create_haystack_pipeline_retrieval_async(
                pipeline_name, username
            )
        else:
            raise ValueError(
                f"Invalid pipeline_type: {pipeline_type}. Must be 'indexing' or 'retrieval'"
            )

    def create_haystack_pipeline_indexing(
        self, pipeline_name: str, username: str
    ) -> Any:
        """
        Create an indexing Haystack Pipeline from components by connecting them sequentially.

        Args:
            pipeline_name: Name of the pipeline to create

        Returns:
            Connected Haystack Pipeline object ready to run

        Raises:
            RuntimeError: If components haven't been built yet
        """
        from haystack import Pipeline

        if pipeline_name not in self._haystack_components_by_pipeline:
            raise RuntimeError(
                f"Components for pipeline '{pipeline_name}' not built yet. "
                f"Call build_haystack_components_from_graph() first."
            )

        # Get the graph data to determine component order
        if pipeline_name not in self._pipeline_graphs:
            raise RuntimeError(f"Graph data for pipeline '{pipeline_name}' not found.")

        components_data = self._pipeline_graphs[pipeline_name]
        haystack_components = self._haystack_components_by_pipeline[pipeline_name]

        # Create Haystack pipeline
        pipeline = Pipeline()

        self.logger.debug(f"Creating indexing Haystack pipeline for: {pipeline_name}")

        # Add all components from the graph (filter out only DocumentStore nodes)
        component_nodes = [
            comp
            for comp in components_data
            if "DocumentStore" not in comp.get("node_labels", [])
        ]

        # Add components to pipeline using comp_id as unique names
        for comp_data in component_nodes:
            comp_id = comp_data.get("id")
            comp_name = comp_data.get("component_name")

            if comp_id in haystack_components:
                # Use comp_id as the component name to ensure uniqueness
                pipeline.add_component(comp_id, haystack_components[comp_id])
                self.logger.debug(f"Added component: {comp_name} (id: {comp_id})")

        # Connect components based on graph edges
        for comp_data in component_nodes:
            current_id = comp_data.get("id")
            current_name = comp_data.get("component_name")
            next_ids = comp_data.get("next_components", [])

            if current_id in haystack_components:
                for next_id in next_ids:
                    # Only connect if next component exists in haystack_components
                    if next_id in haystack_components:
                        next_data = next(
                            (c for c in component_nodes if c.get("id") == next_id), None
                        )
                        if next_data:
                            next_name = next_data.get("component_name")
                            try:
                                pipeline.connect(current_id, next_id)
                                self.logger.debug(
                                    f"Connected: {current_name} -> {next_name}"
                                )
                            except Exception as e:
                                self.logger.warning(
                                    f"Could not connect {current_name} -> {next_name}: {e}"
                                )

        self.logger.info(f"Indexing pipeline '{pipeline_name}' created successfully")

        # Store the connected pipeline
        self._haystack_pipelines[pipeline_name] = pipeline

        return pipeline

    async def create_haystack_pipeline_indexing_async(
        self, pipeline_name: str, username: str
    ) -> Any:
        """
        Async version of create_haystack_pipeline_indexing.
        """
        from haystack import AsyncPipeline

        if pipeline_name not in self._haystack_components_by_pipeline:
            raise RuntimeError(
                f"Components for pipeline '{pipeline_name}' not built yet. "
                f"Call build_haystack_components_from_graph() first."
            )

        # Get the graph data to determine component order
        if pipeline_name not in self._pipeline_graphs:
            raise RuntimeError(f"Graph data for pipeline '{pipeline_name}' not found.")

        components_data = self._pipeline_graphs[pipeline_name]
        haystack_components = self._haystack_components_by_pipeline[pipeline_name]

        # Create Haystack pipeline
        pipeline = AsyncPipeline()

        self.logger.debug(f"Creating indexing Haystack pipeline for: {pipeline_name}")

        # Add all components from the graph (filter out only DocumentStore nodes)
        component_nodes = [
            comp
            for comp in components_data
            if "DocumentStore" not in comp.get("node_labels", [])
        ]

        # Add components to pipeline using comp_id as unique names
        for comp_data in component_nodes:
            comp_id = comp_data.get("id")
            comp_name = comp_data.get("component_name")

            if comp_id in haystack_components:
                # Use comp_id as the component name to ensure uniqueness
                pipeline.add_component(comp_id, haystack_components[comp_id])
                self.logger.debug(f"Added component: {comp_name} (id: {comp_id})")

        # Connect components based on graph edges
        for comp_data in component_nodes:
            current_id = comp_data.get("id")
            current_name = comp_data.get("component_name")
            next_ids = comp_data.get("next_components", [])

            if current_id in haystack_components:
                for next_id in next_ids:
                    # Only connect if next component exists in haystack_components
                    if next_id in haystack_components:
                        next_data = next(
                            (c for c in component_nodes if c.get("id") == next_id), None
                        )
                        if next_data:
                            next_name = next_data.get("component_name")
                            try:
                                pipeline.connect(current_id, next_id)
                                self.logger.debug(
                                    f"Connected: {current_name} -> {next_name}"
                                )
                            except Exception as e:
                                self.logger.warning(
                                    f"Could not connect {current_name} -> {next_name}: {e}"
                                )

        self.logger.info(f"Indexing pipeline '{pipeline_name}' created successfully")

        # Store the connected pipeline
        self._haystack_pipelines[pipeline_name] = pipeline

        return pipeline

    def create_haystack_pipeline_retrieval(
        self, pipeline_name: str, username: str
    ) -> Dict[str, Any]:
        """
        Create multiple connected Haystack Pipelines from components, one per branch.

        Groups components by branch_id, creates separate Pipeline objects for each branch,
        and stores them in _haystack_pipelines with unique keys.

        Args:
            pipeline_name: Name of the retrieval pipeline to create

        Returns:
            Dictionary mapping branch_id to Haystack Pipeline objects

        Raises:
            RuntimeError: If components haven't been built yet or no branches found

        Updates:
            self._haystack_pipelines: Stores branch pipelines with keys like
                                      "{pipeline_name}_{branch_id}"

        Example Input (from _pipeline_graphs and _haystack_components_by_pipeline):
            _pipeline_graphs['retrieval_pipeline'] = [
                {'id': 'comp_1', 'branch_id': 'pipeline_small', 'next_components': ['comp_2']},
                {'id': 'comp_2', 'branch_id': 'pipeline_small', 'next_components': []},
                {'id': 'comp_3', 'branch_id': 'pipeline_medium', 'next_components': ['comp_4']},
                {'id': 'comp_4', 'branch_id': 'pipeline_medium', 'next_components': []}
            ]

            _haystack_components_by_pipeline['retrieval_pipeline'] = {
                'comp_1': <Embedder(model='all-MiniLM-L6-v2')>,
                'comp_2': <Retriever(root_dir='./data/small')>,
                'comp_3': <Embedder(model='paraphrase-MiniLM-L6-v2')>,
                'comp_4': <Retriever(root_dir='./data/medium')>
            }

        Example Output (stored in _haystack_pipelines):
            _haystack_pipelines = {
                'retrieval_pipeline_pipeline_small': <Pipeline with comp_1 → comp_2>,
                'retrieval_pipeline_pipeline_medium': <Pipeline with comp_3 → comp_4>
            }

        Process:
            1. Group components by branch_id:
               - pipeline_small: [comp_1, comp_2]
               - pipeline_medium: [comp_3, comp_4]

            2. For each branch:
               - Create new Pipeline()
               - Add components from that branch
               - Connect components based on next_components
               - Store with key "{pipeline_name}_{branch_id}"

        Note:
            - Each branch becomes an independent Haystack Pipeline
            - Branches share the same pipeline_name but have different branch_ids
            - Components within a branch are connected based on metadata
        """
        from collections import defaultdict

        from haystack import Pipeline

        if pipeline_name not in self._haystack_components_by_pipeline:
            raise RuntimeError(
                f"Components for pipeline '{pipeline_name}' not built yet. "
                f"Call build_haystack_components_from_graph() first."
            )

        # Get the graph data to determine component order
        if pipeline_name not in self._pipeline_graphs:
            raise RuntimeError(f"Graph data for pipeline '{pipeline_name}' not found.")

        components_data = self._pipeline_graphs[pipeline_name]
        haystack_components = self._haystack_components_by_pipeline[pipeline_name]

        # Group components by branch_id
        component_nodes = [
            comp
            for comp in components_data
            if "DocumentStore" not in comp.get("node_labels", [])
        ]

        branches: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for comp_data in component_nodes:
            branch_id = comp_data.get("branch_id")
            if branch_id:
                branches[branch_id].append(comp_data)
            else:
                # Components without branch_id are shared across all branches
                # We'll handle these separately if needed
                pass

        if not branches:
            raise RuntimeError(
                f"No branches found for retrieval pipeline '{pipeline_name}'. "
                "Retrieval pipelines must have branch_id on components."
            )

        self.logger.info(
            f"Creating {len(branches)} retrieval pipeline branch(es) for '{pipeline_name}'"
        )

        # Create separate pipeline for each branch
        branch_pipelines: Dict[str, Any] = {}

        for branch_id, branch_components in branches.items():
            pipeline = Pipeline()
            branch_key = f"{pipeline_name}_{branch_id}"

            self.logger.debug(
                f"Creating retrieval pipeline branch: {branch_key} "
                f"({len(branch_components)} components)"
            )

            # Add components for this branch
            for comp_data in branch_components:
                comp_id = comp_data.get("id")
                comp_name = comp_data.get("component_name")

                if comp_id in haystack_components:
                    pipeline.add_component(comp_id, haystack_components[comp_id])
                    self.logger.debug(f"  Added component: {comp_name} (id: {comp_id})")

            # Connect components within this branch
            for comp_data in branch_components:
                current_id = comp_data.get("id")
                current_name = comp_data.get("component_name")
                next_ids = comp_data.get("next_components", [])

                if current_id in haystack_components:
                    for next_id in next_ids:
                        if next_id in haystack_components:
                            next_data = next(
                                (
                                    c
                                    for c in branch_components
                                    if c.get("id") == next_id
                                ),
                                None,
                            )
                            if next_data:
                                next_name = next_data.get("component_name")
                                try:
                                    pipeline.connect(current_id, next_id)
                                    self.logger.debug(
                                        f"  Connected: {current_name} -> {next_name}"
                                    )
                                except Exception as e:
                                    self.logger.warning(
                                        f"  Could not connect {current_name} -> {next_name}: {e}"
                                    )

            # Store this branch pipeline
            branch_pipelines[branch_id] = pipeline
            self._haystack_pipelines[branch_key] = pipeline

            self.logger.info(f"Created retrieval pipeline branch: {branch_key}")

        self.logger.info(
            f"All {len(branch_pipelines)} retrieval pipeline branches created for '{pipeline_name}'"
        )

        return branch_pipelines

    async def create_haystack_pipeline_retrieval_async(
        self, pipeline_name: str, username: str
    ) -> Dict[str, Any]:
        """
        Async version of create_haystack_pipeline_retrieval.
        """
        from collections import defaultdict

        from haystack import AsyncPipeline

        if pipeline_name not in self._haystack_components_by_pipeline:
            raise RuntimeError(
                f"Components for pipeline '{pipeline_name}' not built yet. "
                f"Call build_haystack_components_from_graph() first."
            )

        # Get the graph data to determine component order
        if pipeline_name not in self._pipeline_graphs:
            raise RuntimeError(f"Graph data for pipeline '{pipeline_name}' not found.")

        components_data = self._pipeline_graphs[pipeline_name]
        haystack_components = self._haystack_components_by_pipeline[pipeline_name]

        # Group components by branch_id
        component_nodes = [
            comp
            for comp in components_data
            if "DocumentStore" not in comp.get("node_labels", [])
        ]

        branches: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for comp_data in component_nodes:
            branch_id = comp_data.get("branch_id")
            if branch_id:
                branches[branch_id].append(comp_data)
            else:
                # Components without branch_id are shared across all branches
                # We'll handle these separately if needed
                pass

        if not branches:
            raise RuntimeError(
                f"No branches found for retrieval pipeline '{pipeline_name}'. "
                "Retrieval pipelines must have branch_id on components."
            )

        self.logger.info(
            f"Creating {len(branches)} retrieval pipeline branch(es) for '{pipeline_name}'"
        )

        # Create separate pipeline for each branch
        branch_pipelines: Dict[str, Any] = {}

        for branch_id, branch_components in branches.items():
            pipeline = AsyncPipeline()
            branch_key = f"{pipeline_name}_{branch_id}"

            self.logger.debug(
                f"Creating retrieval pipeline branch: {branch_key} "
                f"({len(branch_components)} components)"
            )

            # Add components for this branch
            for comp_data in branch_components:
                comp_id = comp_data.get("id")
                comp_name = comp_data.get("component_name")

                if comp_id in haystack_components:
                    pipeline.add_component(comp_id, haystack_components[comp_id])
                    self.logger.debug(f"  Added component: {comp_name} (id: {comp_id})")

            # Connect components within this branch
            for comp_data in branch_components:
                current_id = comp_data.get("id")
                current_name = comp_data.get("component_name")
                next_ids = comp_data.get("next_components", [])

                if current_id in haystack_components:
                    for next_id in next_ids:
                        if next_id in haystack_components:
                            next_data = next(
                                (
                                    c
                                    for c in branch_components
                                    if c.get("id") == next_id
                                ),
                                None,
                            )
                            if next_data:
                                next_name = next_data.get("component_name")
                                try:
                                    pipeline.connect(current_id, next_id)
                                    self.logger.debug(
                                        f"  Connected: {current_name} -> {next_name}"
                                    )
                                except Exception as e:
                                    self.logger.warning(
                                        f"  Could not connect {current_name} -> {next_name}: {e}"
                                    )

            # Store this branch pipeline
            branch_pipelines[branch_id] = pipeline
            self._haystack_pipelines[branch_key] = pipeline

            self.logger.info(f"Created retrieval pipeline branch: {branch_key}")

        self.logger.info(
            f"All {len(branch_pipelines)} retrieval pipeline branches created for '{pipeline_name}'"
        )

        return branch_pipelines

    def run(
        self,
        pipeline_name: str,
        username: str,
        type: str,
        project: str = "default",
        **kwargs: Any,
    ) -> Any:
        """
        Run a pipeline by name, dispatching to the appropriate execution method.

        Args:
            pipeline_name: Name of the pipeline to run (e.g., 'pdf_indexing_pipeline', 'pdf_retrieval_pipeline')
            username: Username for metrics and logging
            type: Pipeline type - "indexing" or "retrieval"
            project: Project name (defaults to "default")
            **kwargs: Pipeline-specific arguments

        Returns:
            Pipeline execution results
        """
        if type == "indexing" or type == PipelineUsage.INDEXING.value:
            return self._run_indexing_pipeline(
                pipeline_name, username, project, **kwargs
            )
        elif type == "retrieval" or type == PipelineUsage.RETRIEVAL.value:
            return self._run_retrieval_pipeline(
                pipeline_name, username, project, **kwargs
            )
        else:
            raise ValueError(
                f"Unknown pipeline type: {type}. " "Must be 'indexing' or 'retrieval'"
            )

    def _run_indexing_pipeline(
        self, pipeline_name: str, username: str, project: str = "default", **kwargs: Any
    ) -> Any:
        """
        Execute an indexing pipeline.

        Args:
            pipeline_name: Name of the indexing pipeline
            username: Username for metrics and logging
            project: Project name (defaults to "default")
            **kwargs: Pipeline-specific arguments
                - data_path: Path to directory containing PDFs (required)
                - sources: Optional list of specific file paths to process

        Returns:
            Indexing results

        FAIR Compliance:
            - Creates a RunNode for provenance tracking
            - Links all generated DataPieces to the Run
        """
        from pathlib import Path

        # Create metrics collector for this user
        metrics = MetricsCollector(username=username)

        start_time = time.time()
        success = False
        error_msg = None

        # FAIR: Generate run ID and create RunNode
        run_id = generate_run_id()

        try:
            # Get the pipeline
            if pipeline_name not in self._haystack_pipelines:
                raise ValueError(
                    f"Pipeline '{pipeline_name}' not found. "
                    f"Call create_haystack_pipeline('{pipeline_name}') first."
                )

            pipeline = self._haystack_pipelines[pipeline_name]

            # Get data path from kwargs
            data_path = kwargs.get("data_path")
            if not data_path:
                raise ValueError("data_path is required in kwargs")

            data_path_obj = Path(data_path)
            if not data_path_obj.exists():
                raise FileNotFoundError(f"Path not found: {data_path_obj}")

            # Check if it's a file or directory
            input_files: List[Path] = []
            if data_path_obj.is_file():
                # Direct file path
                input_files = [data_path_obj]
            else:
                # Directory - glob for supported files
                file_patterns = ["*.pdf", "*.txt", "*.md", "*.docx", "*.html"]
                for pattern in file_patterns:
                    input_files.extend(data_path_obj.glob(pattern))

                if not input_files:
                    raise FileNotFoundError(
                        f"No supported files found in {data_path_obj}"
                    )

            # FAIR: Create RunNode before execution
            if self.enable_caching:
                self._create_run_node(
                    run_id=run_id,
                    pipeline_name=pipeline_name,
                    username=username,
                    project=project,
                    pipeline_type="indexing",
                )
                # Set run_id on all GatedComponents
                self._set_run_id_on_components(pipeline_name, run_id)

            self.logger.info(
                f"Running indexing pipeline: {pipeline_name} (run_id={run_id})"
            )
            self.logger.info(f"Processing {len(input_files)} file(s)")

            # Run the pipeline with the file sources
            sources = [str(file_path) for file_path in input_files]
            result = pipeline.run({"sources": sources})

            self.logger.info(f"Indexing completed for {len(input_files)} files")

            success = True

            # Log metrics
            end_time = time.time()
            component_count = len(
                self._haystack_components_by_pipeline.get(pipeline_name, {})
            )
            metrics.log_pipeline_execution(
                pipeline_name=pipeline_name,
                start_time=start_time,
                end_time=end_time,
                total_components=component_count,
                success=success,
                metadata={
                    "type": "indexing",
                    "files_processed": len(input_files),
                    "data_path": str(data_path_obj),
                    "run_id": run_id,
                },
            )

            # FAIR: Finalize RunNode
            if self.enable_caching:
                self._finalize_run_node(run_id, success=True)

            return result

        except Exception as e:
            error_msg = str(e)
            success = False

            # FAIR: Finalize RunNode with error
            if self.enable_caching:
                self._finalize_run_node(run_id, success=False, error=error_msg)

            # Log failed metrics
            end_time = time.time()
            component_count = len(
                self._haystack_components_by_pipeline.get(pipeline_name, {})
            )
            metrics.log_pipeline_execution(
                pipeline_name=pipeline_name,
                start_time=start_time,
                end_time=end_time,
                total_components=component_count,
                success=success,
                error=error_msg,
                metadata={"type": "indexing", "run_id": run_id},
            )

            raise

    def _run_retrieval_pipeline(
        self, pipeline_name: str, username: str, project: str = "default", **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Execute all branches of a retrieval pipeline and aggregate results.

        Args:
            pipeline_name: Name of the retrieval pipeline
            username: Username for metrics and logging
            project: Project name (defaults to "default")
            **kwargs: Must include 'query' (str)

        Returns:
            Dict with query, branches (results per branch), documents (all docs combined)
        """
        query = kwargs.get("query")
        if not query:
            raise ValueError("'query' is required for retrieval pipelines")

        self.logger.info(f"Running retrieval pipeline: {pipeline_name}")
        self.logger.info(f"Query: {query}")

        # Find all branch pipelines (e.g., "retrieval_pipeline_pipeline_small")
        branch_pipelines = {}
        for pipeline_key in self._haystack_pipelines.keys():
            if pipeline_key.startswith(f"{pipeline_name}_"):
                branch_id = pipeline_key[len(pipeline_name) + 1 :]
                branch_pipelines[branch_id] = self._haystack_pipelines[pipeline_key]

        if not branch_pipelines:
            raise RuntimeError(
                f"No branch pipelines found for '{pipeline_name}'. "
                "Did you load the pipeline with PipelineRunner?"
            )

        self.logger.info(
            f"Found {len(branch_pipelines)} branch(es): {list(branch_pipelines.keys())}"
        )

        # Run each branch
        branch_results = {}
        for branch_id, pipeline in branch_pipelines.items():
            self.logger.info(f"Running branch: {branch_id}")
            try:
                # Run with query for embedder and prompt_builder
                # This ensures we get documents from the retriever component
                component_ids = (
                    list(pipeline.graph.nodes.keys())
                    if hasattr(pipeline, "graph")
                    else []
                )

                # Build pipeline inputs (including optional evaluation data)
                pipeline_inputs = {
                    "text": query,
                    "query": query,
                }

                # Add evaluation data if provided (for evaluator components)
                if "ground_truth_answer" in kwargs:
                    pipeline_inputs["ground_truth_answer"] = kwargs[
                        "ground_truth_answer"
                    ]
                if "relevant_doc_ids" in kwargs:
                    pipeline_inputs["relevant_doc_ids"] = kwargs["relevant_doc_ids"]

                result = pipeline.run(
                    pipeline_inputs,
                    include_outputs_from=set(component_ids) if component_ids else None,
                )
                branch_results[branch_id] = result
            except Exception as e:
                self.logger.error(f"Error in branch {branch_id}: {e}", exc_info=True)
                branch_results[branch_id] = {"error": str(e)}

        # Aggregate documents from all branches
        all_documents = []
        for branch_id, result in branch_results.items():
            if isinstance(result, dict) and "error" not in result:
                # Find documents in any component output
                for comp_result in result.values():
                    if isinstance(comp_result, dict) and "documents" in comp_result:
                        docs = comp_result["documents"]
                        # Tag each doc with branch_id
                        for doc in docs:
                            if hasattr(doc, "meta"):
                                doc.meta["branch_id"] = branch_id
                            else:
                                doc.meta = {"branch_id": branch_id}
                        all_documents.extend(docs)

        self.logger.info(
            f"Retrieval completed: {len(all_documents)} documents from {len(branch_pipelines)} branches"
        )

        return {
            "query": query,
            "branches": branch_results,
            "documents": all_documents,
            "total_documents": len(all_documents),
            "branches_count": len(branch_pipelines),
        }

    async def run_async(
        self,
        pipeline_name: str,
        username: str,
        type: str,
        project: str = "default",
        **kwargs: Any,
    ) -> Any:
        """
        Async version of run.

        Run a pipeline by name, dispatching to the appropriate execution method.

        Args:
            pipeline_name: Name of the pipeline to run
            username: Username for metrics and logging
            type: Pipeline type - "indexing" or "retrieval"
            project: Project name (defaults to "default")
            **kwargs: Pipeline-specific arguments

        Returns:
            Pipeline execution results
        """
        if type == "indexing" or type == PipelineUsage.INDEXING.value:
            return await self._run_indexing_pipeline_async(
                pipeline_name, username, project, **kwargs
            )
        elif type == "retrieval" or type == PipelineUsage.RETRIEVAL.value:
            return await self._run_retrieval_pipeline_async(
                pipeline_name, username, project, **kwargs
            )
        else:
            raise ValueError(
                f"Unknown pipeline type: {type}. Must be 'indexing' or 'retrieval'"
            )

    async def _run_indexing_pipeline_async(
        self, pipeline_name: str, username: str, project: str = "default", **kwargs: Any
    ) -> Any:
        """
        Async version of _run_indexing_pipeline.

        Executes indexing pipeline using AsyncPipeline.run_async().

        FAIR Compliance:
            - Creates a RunNode for provenance tracking
            - Links all generated DataPieces to the Run
        """
        from pathlib import Path

        # Create metrics collector for this user
        metrics = MetricsCollector(username=username)

        start_time = time.time()
        success = False
        error_msg = None

        # FAIR: Generate run ID
        run_id = generate_run_id()

        try:
            # Get the pipeline
            if pipeline_name not in self._haystack_pipelines:
                raise ValueError(
                    f"Pipeline '{pipeline_name}' not found. "
                    f"Call create_haystack_pipeline('{pipeline_name}') first."
                )

            pipeline = self._haystack_pipelines[pipeline_name]

            # Get data path from kwargs
            data_path = kwargs.get("data_path")
            if not data_path:
                raise ValueError("data_path is required in kwargs")

            data_path_obj = Path(data_path)
            if not data_path_obj.exists():
                raise FileNotFoundError(f"Path not found: {data_path_obj}")

            # Check if it's a file or directory
            input_files: List[Path] = []
            if data_path_obj.is_file():
                # Direct file path
                input_files = [data_path_obj]
            else:
                # Directory - glob for supported files
                file_patterns = ["*.pdf", "*.txt", "*.md", "*.docx", "*.html"]
                for pattern in file_patterns:
                    input_files.extend(data_path_obj.glob(pattern))

                if not input_files:
                    raise FileNotFoundError(
                        f"No supported files found in {data_path_obj}"
                    )

            # FAIR: Create RunNode before execution
            if self.enable_caching:
                await self._create_run_node_async(
                    run_id=run_id,
                    pipeline_name=pipeline_name,
                    username=username,
                    project=project,
                    pipeline_type="indexing",
                )
                # Set run_id on all GatedComponents
                self._set_run_id_on_components(pipeline_name, run_id)

            self.logger.info(
                f"Running indexing pipeline (async): {pipeline_name} (run_id={run_id})"
            )
            self.logger.info(f"Processing {len(input_files)} file(s)")

            # Run the pipeline with the file sources
            # Use AsyncPipeline.run_async() for async execution
            sources = [str(file_path) for file_path in input_files]
            result = await pipeline.run_async(data={"sources": sources})

            self.logger.info(f"Indexing completed for {len(input_files)} files")

            success = True

            # Log metrics
            end_time = time.time()
            component_count = len(
                self._haystack_components_by_pipeline.get(pipeline_name, {})
            )
            metrics.log_pipeline_execution(
                pipeline_name=pipeline_name,
                start_time=start_time,
                end_time=end_time,
                total_components=component_count,
                success=success,
                metadata={
                    "type": "indexing",
                    "files_processed": len(input_files),
                    "data_path": str(data_path_obj),
                    "mode": "async",
                    "run_id": run_id,
                },
            )

            # FAIR: Finalize RunNode
            if self.enable_caching:
                await self._finalize_run_node_async(run_id, success=True)

            return result

        except Exception as e:
            error_msg = str(e)
            success = False

            # FAIR: Finalize RunNode with error
            if self.enable_caching:
                await self._finalize_run_node_async(
                    run_id, success=False, error=error_msg
                )

            # Log failed metrics
            end_time = time.time()
            component_count = len(
                self._haystack_components_by_pipeline.get(pipeline_name, {})
            )
            metrics.log_pipeline_execution(
                pipeline_name=pipeline_name,
                start_time=start_time,
                end_time=end_time,
                total_components=component_count,
                success=success,
                error=error_msg,
                metadata={"type": "indexing", "mode": "async", "run_id": run_id},
            )

            raise

    async def _run_retrieval_pipeline_async(
        self, pipeline_name: str, username: str, project: str = "default", **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Async version of _run_retrieval_pipeline.

        Executes retrieval pipeline branches concurrently where possible.
        """
        import asyncio

        query = kwargs.get("query")
        if not query:
            raise ValueError("'query' is required for retrieval pipelines")

        self.logger.info(f"Running retrieval pipeline (async): {pipeline_name}")
        self.logger.info(f"Query: {query}")

        # Find all branch pipelines
        branch_pipelines = {}
        for pipeline_key in self._haystack_pipelines.keys():
            if pipeline_key.startswith(f"{pipeline_name}_"):
                branch_id = pipeline_key[len(pipeline_name) + 1 :]
                branch_pipelines[branch_id] = self._haystack_pipelines[pipeline_key]

        if not branch_pipelines:
            raise RuntimeError(
                f"No branch pipelines found for '{pipeline_name}'. "
                "Did you load the pipeline with PipelineRunner?"
            )

        self.logger.info(
            f"Found {len(branch_pipelines)} branch(es): {list(branch_pipelines.keys())}"
        )

        # Define helper to run a single branch
        async def run_branch(branch_id: str, pipeline: Any) -> Any:
            self.logger.info(f"Running branch (async): {branch_id}")
            try:
                component_ids = (
                    list(pipeline.graph.nodes.keys())
                    if hasattr(pipeline, "graph")
                    else []
                )

                pipeline_inputs = {
                    "text": query,
                    "query": query,
                }

                if "ground_truth_answer" in kwargs:
                    pipeline_inputs["ground_truth_answer"] = kwargs[
                        "ground_truth_answer"
                    ]
                if "relevant_doc_ids" in kwargs:
                    pipeline_inputs["relevant_doc_ids"] = kwargs["relevant_doc_ids"]

                # Run pipeline using AsyncPipeline.run_async()
                return await pipeline.run_async(
                    data=pipeline_inputs,
                    include_outputs_from=set(component_ids) if component_ids else None,
                )
            except Exception as e:
                self.logger.error(f"Error in branch {branch_id}: {e}", exc_info=True)
                return {"error": str(e)}

        # Run all branches concurrently
        branch_tasks = [
            run_branch(branch_id, pipeline)
            for branch_id, pipeline in branch_pipelines.items()
        ]
        results = await asyncio.gather(*branch_tasks)

        # Map results back to branch_ids
        branch_results = dict(zip(branch_pipelines.keys(), results))

        # Aggregate documents
        all_documents = []
        for branch_id, result in branch_results.items():
            if isinstance(result, dict) and "error" not in result:
                for comp_result in result.values():
                    if isinstance(comp_result, dict) and "documents" in comp_result:
                        docs = comp_result["documents"]
                        for doc in docs:
                            if hasattr(doc, "meta"):
                                doc.meta["branch_id"] = branch_id
                            else:
                                doc.meta = {"branch_id": branch_id}
                        all_documents.extend(docs)

        self.logger.info(
            f"Retrieval completed (async): {len(all_documents)} documents from {len(branch_pipelines)} branches"
        )

        return {
            "query": query,
            "branches": branch_results,
            "documents": all_documents,
            "total_documents": len(all_documents),
            "branches_count": len(branch_pipelines),
        }
