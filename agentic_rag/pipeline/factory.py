"""Factory for creating pipelines from specifications."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ..components import GraphStore, get_default_registry
from ..types import (
    PipelineSpec,
    PipelineType,
    get_component_value,
    validate_component_spec,
)
from ..utils.logger import configure_haystack_logging, get_logger
from .storage import GraphStorage

if TYPE_CHECKING:
    from ..config import Config


class PipelineFactory:
    """
    Factory for creating pipelines from component specifications (Singleton).

    Usage:
        ```python
        # With Config object (SDK style)
        from agentic_rag import Config
        config = Config(
            neo4j_uri="bolt://localhost:7687",
            neo4j_username="neo4j",
            neo4j_password="password",
            openrouter_api_key="your-key"
        )
        factory = PipelineFactory(config=config)

        # Build pipelines with username injection
        pipeline_specs = factory.build_pipeline_graphs_from_specs(
            pipeline_specs=[...],
            username="alice"
        )
        ```
    """

    _instance: Optional["PipelineFactory"] = None
    _initialized: bool = False
    graph_store: Optional[GraphStore]

    def __new__(cls, *args: Any, **kwargs: Any) -> "PipelineFactory":
        """Ensure only one instance of PipelineFactory exists."""
        if cls._instance is None:
            cls._instance = super(PipelineFactory, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        graph_store: Optional[GraphStore] = None,
        config: Optional["Config"] = None,
    ) -> None:
        """
        Initialize PipelineFactory (singleton).

        Args:
            graph_store: Optional GraphStore instance (will be created from config if not provided)
            config: Config object with credentials and settings (falls back to env vars)

        Note:
            This is a singleton class. Only the first initialization will be used.
            Username is now injected at method level for multi-tenant isolation.
        """
        # Only initialize once
        if self._initialized:
            return

        self.registry = get_default_registry()
        self.config = config

        # Initialize graph_store from config if not provided
        if graph_store is None and config is not None and config.validate_neo4j():
            self.graph_store = GraphStore(config=config)
        else:
            self.graph_store = graph_store

        self.graph_storage = (
            GraphStorage(self.graph_store, self.registry) if self.graph_store else None
        )
        self.logger = get_logger(__name__, config=config)

        # Configure Haystack logging
        log_level = config.log_level if config else "DEBUG"
        configure_haystack_logging(level=log_level)

        self._initialized = True

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton instance (useful for testing)."""
        cls._instance = None
        cls._initialized = False

    def build_pipeline_graphs_from_specs(
        self,
        pipeline_specs: List[List[Dict[str, str]]],
        username: str,
        project: str = "default",
        configs: Optional[List[Dict[str, Any]]] = None,
        pipeline_types: Optional[List[str]] = None,
    ) -> List[PipelineSpec]:
        """
        Build multiple pipeline graphs from dict-based specifications.

        Args:
            pipeline_specs: List of component specifications as dicts.
                Example: [[{"type": "CONVERTER.PDF"}, {"type": "CHUNKER.RECURSIVE"}]]
            username: Username for multi-tenant isolation
            project: Project name (defaults to "default")
            configs: Optional list of configuration dicts for each pipeline
            pipeline_types: Optional list of pipeline types ("indexing" or "retrieval")
                Defaults to "indexing" for all pipelines

        Returns:
            List of PipelineSpec objects with graph representations built
        """
        if configs is None:
            configs = [{}] * len(pipeline_specs)

        if pipeline_types is None:
            pipeline_types = ["indexing"] * len(pipeline_specs)

        if len(configs) != len(pipeline_specs):
            raise ValueError("Number of configs must match number of pipeline specs")

        if len(pipeline_types) != len(pipeline_specs):
            raise ValueError(
                "Number of pipeline_types must match number of pipeline specs"
            )

        self.logger.info(
            f"Building {len(pipeline_specs)} pipeline graphs for user: {username}, project: {project}"
        )

        pipeline_specs_list = []

        for i, (spec, config, pipeline_type) in enumerate(
            zip(pipeline_specs, configs, pipeline_types)
        ):
            if len(spec) < 1:
                raise ValueError(
                    f"Pipeline {i} must have at least 1 component, got {len(spec)}"
                )

            # Use custom pipeline name from config if provided, otherwise default to pipeline_{i}
            pipeline_name = config.get("_pipeline_name", f"pipeline_{i}")
            self.logger.debug(f"Building {pipeline_type} pipeline {i}: {pipeline_name}")
            pipeline_spec = self.build_pipeline_graph(
                spec, pipeline_name, username, project, config, pipeline_type
            )
            pipeline_specs_list.append(pipeline_spec)

        self.logger.info(f"Successfully built {len(pipeline_specs_list)} pipelines")
        return pipeline_specs_list

    async def build_pipeline_graphs_from_specs_async(
        self,
        pipeline_specs: List[List[Dict[str, str]]],
        username: str,
        project: str = "default",
        configs: Optional[List[Dict[str, Any]]] = None,
        pipeline_types: Optional[List[str]] = None,
    ) -> List[PipelineSpec]:
        """
        Async version of build_pipeline_graphs_from_specs.

        Build multiple pipeline graphs from dict-based specifications.

        Args:
            pipeline_specs: List of component specifications as dicts.
            username: Username for multi-tenant isolation
            project: Project name (defaults to "default")
            configs: Optional list of configuration dicts for each pipeline
            pipeline_types: Optional list of pipeline types ("indexing" or "retrieval")

        Returns:
            List of PipelineSpec objects with graph representations built
        """
        if configs is None:
            configs = [{}] * len(pipeline_specs)

        if pipeline_types is None:
            pipeline_types = ["indexing"] * len(pipeline_specs)

        if len(configs) != len(pipeline_specs):
            raise ValueError("Number of configs must match number of pipeline specs")

        if len(pipeline_types) != len(pipeline_specs):
            raise ValueError(
                "Number of pipeline_types must match number of pipeline specs"
            )

        self.logger.info(
            f"Building {len(pipeline_specs)} pipeline graphs (async) for user: {username}, project: {project}"
        )

        pipeline_specs_list = []

        for i, (spec, config, pipeline_type) in enumerate(
            zip(pipeline_specs, configs, pipeline_types)
        ):
            if len(spec) < 1:
                raise ValueError(
                    f"Pipeline {i} must have at least 1 component, got {len(spec)}"
                )

            # Use custom pipeline name from config if provided, otherwise default to pipeline_{i}
            pipeline_name = config.get("_pipeline_name", f"pipeline_{i}")
            self.logger.debug(
                f"Building {pipeline_type} pipeline (async) {i}: {pipeline_name}"
            )
            pipeline_spec = await self.build_pipeline_graph_async(
                spec, pipeline_name, username, project, config, pipeline_type
            )
            pipeline_specs_list.append(pipeline_spec)

        self.logger.info(
            f"Successfully built {len(pipeline_specs_list)} pipelines (async)"
        )
        return pipeline_specs_list

    def build_pipeline_graph(
        self,
        component_specs: List[Dict[str, str]],
        pipeline_name: str,
        username: str,
        project: str = "default",
        config: Optional[Dict[str, Any]] = None,
        pipeline_type: str = "indexing",
    ) -> PipelineSpec:
        """
        Build a single pipeline graph from dict-based component specifications.

        Args:
            component_specs: List of component specifications as dicts
                Example: [{"type": "CONVERTER.PDF"}, {"type": "CHUNKER.RECURSIVE"}]
            pipeline_name: Name for the pipeline
            username: Username for multi-tenant isolation
            project: Project name (defaults to "default")
            config: Optional configuration dict
            pipeline_type: Type of pipeline - "indexing" or "retrieval" (default: "indexing")

        Returns:
            PipelineSpec with graph representation built
        """
        config = config or {}

        # Route to appropriate builder based on pipeline type
        if pipeline_type == "indexing":
            return self._build_indexing_pipeline(
                component_specs, pipeline_name, username, project, config
            )
        elif pipeline_type == "retrieval":
            return self._build_retrieval_pipeline(
                component_specs, pipeline_name, username, project, config
            )
        else:
            raise ValueError(
                f"Invalid pipeline_type: {pipeline_type}. Must be 'indexing' or 'retrieval'"
            )

    async def build_pipeline_graph_async(
        self,
        component_specs: List[Dict[str, str]],
        pipeline_name: str,
        username: str,
        project: str = "default",
        config: Optional[Dict[str, Any]] = None,
        pipeline_type: str = "indexing",
    ) -> PipelineSpec:
        """
        Async version of build_pipeline_graph.

        Build a single pipeline graph from dict-based component specifications.

        Args:
            component_specs: List of component specifications as dicts
            pipeline_name: Name for the pipeline
            username: Username for multi-tenant isolation
            project: Project name (defaults to "default")
            config: Optional configuration dict
            pipeline_type: Type of pipeline - "indexing" or "retrieval" (default: "indexing")

        Returns:
            PipelineSpec with graph representation built
        """
        config = config or {}

        # Route to appropriate builder based on pipeline type
        if pipeline_type == "indexing":
            return await self._build_indexing_pipeline_async(
                component_specs, pipeline_name, username, project, config
            )
        elif pipeline_type == "retrieval":
            return await self._build_retrieval_pipeline_async(
                component_specs, pipeline_name, username, project, config
            )
        else:
            raise ValueError(
                f"Invalid pipeline_type: {pipeline_type}. Must be 'indexing' or 'retrieval'"
            )

    def _build_indexing_pipeline(
        self,
        component_specs: List[Dict[str, str]],
        pipeline_name: str,
        username: str,
        project: str,
        config: Dict[str, Any],
        branch_id: Optional[str] = None,
        pipeline_type: Optional[PipelineType] = None,
    ) -> PipelineSpec:
        """
        Build an indexing pipeline.

        Args:
            component_specs: List of component specifications
            pipeline_name: Name for the pipeline
            username: Username for multi-tenant isolation
            project: Project name
            config: Configuration dict
            branch_id: Optional branch identifier for retrieval pipeline branches
            pipeline_type: Optional pipeline type override

        Returns:
            PipelineSpec for indexing pipeline
        """
        self.logger.info(
            f"Building indexing pipeline: {pipeline_name} for user: {username}, project: {project}"
        )

        # Parse component specifications and validate
        component_specs_list = []
        for spec_item in component_specs:
            component_name = self._parse_component_spec(spec_item)

            spec = self.registry.get_component_spec(component_name)
            if spec is None:
                raise ValueError(f"Unknown component: {component_name}")

            # Configure the spec directly with user config
            user_config = config.get(component_name, {})

            # Auto-generate root_dir for document writers if not provided
            if (
                component_name in ["chroma_document_writer", "qdrant_document_writer"]
                and "root_dir" not in user_config
            ):
                user_config = user_config.copy()  # Don't modify original config
                # Use agentic_root_dir from config if available
                root_dir = self.config.agentic_root_dir if self.config else "./data"
                user_config["root_dir"] = (
                    f"{root_dir}/{username}/{project}/{pipeline_name}"
                )
                self.logger.debug(
                    f"Auto-generated root_dir for {component_name}: {user_config['root_dir']}"
                )

            configured_spec = spec.configure(user_config)

            # Store the original full type string
            configured_spec.full_type = spec_item.get("type", "")

            component_specs_list.append(configured_spec)

        # Create pipeline specification
        pipeline_spec = PipelineSpec(
            name=pipeline_name,
            components=component_specs_list,
            pipeline_type=pipeline_type or PipelineType.INDEXING,
        )

        # Build the graph representation
        if self.graph_storage:
            self.logger.info(
                f"Creating graph representation for indexing pipeline '{pipeline_name}'"
            )
            self.graph_storage.build_pipeline_graph(
                pipeline_spec, username, project, branch_id
            )
        else:
            self.logger.warning("No graph store configured, pipeline graph not created")

        return pipeline_spec

    async def _build_indexing_pipeline_async(
        self,
        component_specs: List[Dict[str, str]],
        pipeline_name: str,
        username: str,
        project: str,
        config: Dict[str, Any],
        branch_id: Optional[str] = None,
        pipeline_type: Optional[PipelineType] = None,
    ) -> PipelineSpec:
        """
        Async version of _build_indexing_pipeline.

        Build an indexing pipeline.

        Args:
            component_specs: List of component specifications
            pipeline_name: Name for the pipeline
            username: Username for multi-tenant isolation
            project: Project name
            config: Configuration dict
            branch_id: Optional branch identifier for retrieval pipeline branches
            pipeline_type: Optional pipeline type override

        Returns:
            PipelineSpec for indexing pipeline
        """
        self.logger.info(
            f"Building indexing pipeline (async): {pipeline_name} for user: {username}, project: {project}"
        )
        self.logger.info(f"Component specs: {component_specs}")

        # Parse component specifications and validate
        component_specs_list = []
        for spec_item in component_specs:
            try:
                component_name = self._parse_component_spec(spec_item)
                self.logger.info(f"Processing component: {component_name}")

                spec = self.registry.get_component_spec(component_name)
                if spec is None:
                    raise ValueError(f"Unknown component: {component_name}")

                # Configure the spec directly with user config
                user_config = config.get(component_name, {})

                # Auto-generate root_dir for document writers if not provided
                if (
                    component_name in ["chroma_document_writer", "qdrant_document_writer"]
                    and "root_dir" not in user_config
                ):
                    user_config = user_config.copy()  # Don't modify original config
                    # Use agentic_root_dir from config if available
                    root_dir = self.config.agentic_root_dir if self.config else "./data"
                    user_config["root_dir"] = (
                        f"{root_dir}/{username}/{project}/{pipeline_name}"
                    )
                    self.logger.info(
                        f"Auto-generated root_dir for {component_name}: {user_config['root_dir']}"
                    )

                configured_spec = spec.configure(user_config)

                # Store the original full type string
                configured_spec.full_type = spec_item.get("type", "")

                component_specs_list.append(configured_spec)
            except Exception as e:
                self.logger.error(f"Error processing component spec {spec_item}: {e}")
                raise

        self.logger.info("Finished processing components, creating PipelineSpec")
        # Create pipeline specification
        pipeline_spec = PipelineSpec(
            name=pipeline_name,
            components=component_specs_list,
            pipeline_type=pipeline_type or PipelineType.INDEXING,
        )

        # Build the graph representation
        if self.graph_storage:
            self.logger.info(
                f"Creating graph representation (async) for indexing pipeline '{pipeline_name}'"
            )
            await self.graph_storage.build_pipeline_graph_async(
                pipeline_spec, username, project, branch_id
            )
        else:
            self.logger.warning("No graph store configured, pipeline graph not created")

        return pipeline_spec

    def _extract_indexing_pipelines(self, config: Dict[str, Any]) -> List[str]:
        """Step 1: Extract indexing pipeline names from config."""
        indexing_pipelines = config.get("_indexing_pipelines", [])

        if not indexing_pipelines:
            raise ValueError(
                "Retrieval pipeline requires '_indexing_pipelines' in config. "
                "Example: {'_indexing_pipelines': ['pipeline_0']}"
            )

        if not isinstance(indexing_pipelines, list):
            raise ValueError(
                f"'_indexing_pipelines' must be a list, got: {type(indexing_pipelines)}"
            )

        self.logger.info(
            f"Will query {len(indexing_pipelines)} indexing pipeline(s): {indexing_pipelines}"
        )

        return indexing_pipelines

    def _fetch_indexing_pipeline_components(
        self, indexing_pipelines: List[str], username: str, project: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Step 2: Fetch embedder and writer components from each indexing pipeline.

        We query all components for each pipeline and filter for embedder/writer pairs.

        Args:
            indexing_pipelines: List of pipeline names to fetch components from
            username: Username for multi-tenant isolation
            project: Project name for isolation
        """
        if not self.graph_store:
            raise RuntimeError("Cannot build retrieval pipeline without a graph store")

        indexing_pipeline_components: Dict[str, List[Dict[str, Any]]] = {}

        for indexing_pipeline_name in indexing_pipelines:
            self.logger.debug(f"Querying components for: {indexing_pipeline_name}")

            # Get all components for this pipeline with project filtering
            all_components = self.graph_store.get_components_by_pipeline(
                pipeline_name=indexing_pipeline_name, username=username, project=project
            )

            if not all_components:
                raise ValueError(
                    f"No components found for '{indexing_pipeline_name}' (user: {username}, project: {project})"
                )

            # Filter for embedder and writer components (needed for retrieval)
            # Each indexing pipeline should have exactly one embedder and one writer
            embedder = None
            writer = None

            for component in all_components:
                component_name = component.get("component_name", "")
                if "embedder" in component_name.lower():
                    embedder = component
                elif "writer" in component_name.lower():
                    writer = component

            # Combine in order: embedder first, then writer
            relevant_components = [c for c in [embedder, writer] if c is not None]

            if not relevant_components:
                raise ValueError(
                    f"No embedder/writer components found for '{indexing_pipeline_name}' in project '{project}'. "
                    f"Make sure the indexing pipeline exists in the same project before creating a retrieval pipeline."
                )

            indexing_pipeline_components[indexing_pipeline_name] = relevant_components

            component_names = [c.get("component_name") for c in relevant_components]
            self.logger.info(
                f"Retrieved {len(relevant_components)} component(s) for "
                f"'{indexing_pipeline_name}': {component_names}"
            )

        return indexing_pipeline_components

    async def _fetch_indexing_pipeline_components_async(
        self, indexing_pipelines: List[str], username: str, project: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Async version of _fetch_indexing_pipeline_components.

        Step 2: Fetch embedder and writer components from each indexing pipeline.

        Args:
            indexing_pipelines: List of pipeline names to fetch components from
            username: Username for multi-tenant isolation
            project: Project name for isolation
        """
        if not self.graph_store:
            raise RuntimeError("Cannot build retrieval pipeline without a graph store")

        indexing_pipeline_components: Dict[str, List[Dict[str, Any]]] = {}

        for indexing_pipeline_name in indexing_pipelines:
            self.logger.debug(
                f"Querying components (async) for: {indexing_pipeline_name}"
            )

            # Get all components for this pipeline with project filtering (async)
            all_components = await self.graph_store.get_components_by_pipeline_async(
                pipeline_name=indexing_pipeline_name, username=username, project=project
            )

            if not all_components:
                raise ValueError(
                    f"No components found for '{indexing_pipeline_name}' (user: {username}, project: {project})"
                )

            # Filter for embedder and writer components (needed for retrieval)
            # Each indexing pipeline should have exactly one embedder and one writer
            embedder = None
            writer = None

            for component in all_components:
                component_name = component.get("component_name", "")
                if "embedder" in component_name.lower():
                    embedder = component
                elif "writer" in component_name.lower():
                    writer = component

            # Combine in order: embedder first, then writer
            relevant_components = [c for c in [embedder, writer] if c is not None]

            if not relevant_components:
                raise ValueError(
                    f"No embedder/writer components found for '{indexing_pipeline_name}' in project '{project}'. "
                    f"Make sure the indexing pipeline exists in the same project before creating a retrieval pipeline."
                )

            indexing_pipeline_components[indexing_pipeline_name] = relevant_components

            component_names = [c.get("component_name") for c in relevant_components]
            self.logger.info(
                f"Retrieved {len(relevant_components)} component(s) for "
                f"'{indexing_pipeline_name}': {component_names}"
            )

        return indexing_pipeline_components

    def _substitute_components_for_retrieval(self, component_type: str) -> str:
        """
        Substitute indexing components with retrieval equivalents.

        Maps:
        - EMBEDDER.SENTENCE_TRANSFORMERS_DOC → EMBEDDER.SENTENCE_TRANSFORMERS (text embedder for queries)
        - WRITER.CHROMA_DOCUMENT_WRITER → RETRIEVER.CHROMA_EMBEDDING

        Args:
            component_type: Original component type from indexing pipeline

        Returns:
            Substituted component type for retrieval, or original if no mapping
        """
        # Define substitution mappings
        RETRIEVAL_SUBSTITUTIONS = {
            "EMBEDDER.SENTENCE_TRANSFORMERS_DOC": "EMBEDDER.SENTENCE_TRANSFORMERS",  # Doc embedder → Text embedder
            "WRITER.CHROMA_DOCUMENT_WRITER": "RETRIEVER.CHROMA_EMBEDDING",
            "WRITER.QDRANT_DOCUMENT_WRITER": "RETRIEVER.QDRANT_EMBEDDING",
            # Add more mappings here as needed
        }

        substituted = RETRIEVAL_SUBSTITUTIONS.get(component_type, component_type)

        if substituted != component_type:
            self.logger.debug(f"  Substituted: {component_type} → {substituted}")

        return substituted

    def _build_component_specs(
        self,
        component_specs: List[Dict[str, str]],
        indexing_pipeline_components: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, List[Dict[str, str]]]:
        """Step 4: Build component specs for each indexing pipeline."""
        retrieval_pipelines_specs = {}

        for (
            indexing_pipeline_name,
            components_from_index,
        ) in indexing_pipeline_components.items():
            pipeline_component_specs = []

            for spec_item in component_specs:
                spec_type = spec_item.get("type", "")

                if spec_type == "INDEX":
                    for component_node in components_from_index:
                        component_type = component_node.get("component_type")
                        if not component_type:
                            continue

                        # Substitute component for retrieval (e.g., Writer → Retriever)
                        retrieval_component_type = (
                            self._substitute_components_for_retrieval(component_type)
                        )
                        pipeline_component_specs.append(
                            {"type": retrieval_component_type}
                        )
                else:
                    pipeline_component_specs.append(spec_item)

            retrieval_pipelines_specs[indexing_pipeline_name] = pipeline_component_specs

        return retrieval_pipelines_specs

    def _build_configs(
        self,
        config: Dict[str, Any],
        indexing_pipeline_components: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Dict[str, Any]]:
        """Step 5: Build configs for each indexing pipeline."""
        import json as json_module

        retrieval_pipelines_configs = {}

        for (
            indexing_pipeline_name,
            components_from_index,
        ) in indexing_pipeline_components.items():
            # Copy config but remove _pipeline_name (we pass it as parameter instead)
            pipeline_config = {k: v for k, v in config.items() if k != "_pipeline_name"}

            for component_node in components_from_index:
                component_name = component_node.get("component_name", "")
                component_type = component_node.get("component_type", "")
                component_config_json = component_node.get(
                    "component_config_json", "{}"
                )

                component_config = json_module.loads(component_config_json)

                # Substitute component type for retrieval (e.g., Writer → Retriever)
                retrieval_component_type = self._substitute_components_for_retrieval(
                    component_type
                )

                # Get the retrieval component name from the substituted type
                if retrieval_component_type != component_type:
                    # Parse the retrieval type to get the actual component name
                    retrieval_component_name = get_component_value(
                        retrieval_component_type
                    )

                    # Merge configs: start with indexing component's config
                    merged_config = component_config.copy()

                    # Override with user-provided config for the retrieval component
                    # This allows user to override model, top_k, etc. while keeping root_dir
                    user_config = config.get(retrieval_component_name, {})
                    merged_config.update(user_config)

                    # Store under the retrieval component's name
                    pipeline_config[retrieval_component_name] = merged_config
                else:
                    # No substitution, use original name
                    pipeline_config[component_name] = component_config

            retrieval_pipelines_configs[indexing_pipeline_name] = pipeline_config

        return retrieval_pipelines_configs

    def _build_retrieval_pipeline(
        self,
        component_specs: List[Dict[str, str]],
        pipeline_name: str,
        username: str,
        project: str,
        config: Dict[str, Any],
    ) -> PipelineSpec:
        """
        Build a retrieval pipeline.

        Retrieval pipelines are built dynamically from indexing pipeline metadata.
        Components like embedders and retrievers are auto-injected based on
        the indexing pipelines they query.

        Args:
            component_specs: List of component specifications (e.g., generator)
            pipeline_name: Name for the pipeline
            username: Username for multi-tenant isolation
            project: Project name
            config: Configuration dict with optional "_indexing_pipelines" key

        Returns:
            PipelineSpec for retrieval pipeline

        Example config:
            {
                "_indexing_pipelines": ["pipeline_0"],  # Which indexing pipelines to query
                "generator": {"model": "gpt-4"}
            }
        """
        self.logger.info(
            f"Building retrieval pipeline: {pipeline_name} for user: {username}, project: {project}"
        )

        # Step 1: Extract indexing pipeline names
        indexing_pipelines = self._extract_indexing_pipelines(config)

        # Step 2: Fetch embedder and writer components
        indexing_pipeline_components = self._fetch_indexing_pipeline_components(
            indexing_pipelines, username, project
        )

        # Step 3: Build component specs for each pipeline
        retrieval_pipelines_specs = self._build_component_specs(
            component_specs, indexing_pipeline_components
        )

        # Step 4: Build configs for each pipeline
        retrieval_pipelines_configs = self._build_configs(
            config, indexing_pipeline_components
        )

        # Step 5: Build N pipelines using standard indexing builder
        # All branches share same pipeline name but have unique component IDs via branch_id
        built_pipelines = []

        for indexing_pipeline_name in retrieval_pipelines_specs.keys():
            pipeline_spec = retrieval_pipelines_specs[indexing_pipeline_name]
            pipeline_config = retrieval_pipelines_configs[indexing_pipeline_name]

            # Build pipeline using indexing builder with branch_id
            # Set pipeline_type to RETRIEVAL for retrieval branches
            built_pipeline = self._build_indexing_pipeline(
                component_specs=pipeline_spec,
                pipeline_name=pipeline_name,
                username=username,
                project=project,
                config=pipeline_config,
                branch_id=indexing_pipeline_name,
                pipeline_type=PipelineType.RETRIEVAL,
            )

            built_pipeline.indexing_pipelines = [indexing_pipeline_name]
            built_pipelines.append(built_pipeline)

        self.logger.info(
            f"Built {len(built_pipelines)} retrieval pipeline branch(es) for '{pipeline_name}'"
        )

        # Return the first one (they all have the same name)
        if not built_pipelines:
            raise RuntimeError(
                f"Failed to build any retrieval pipeline branches for '{pipeline_name}'"
            )

        return built_pipelines[0]

    async def _build_retrieval_pipeline_async(
        self,
        component_specs: List[Dict[str, str]],
        pipeline_name: str,
        username: str,
        project: str,
        config: Dict[str, Any],
    ) -> PipelineSpec:
        """
        Async version of _build_retrieval_pipeline.

        Build a retrieval pipeline.

        Args:
            component_specs: List of component specifications (e.g., generator)
            pipeline_name: Name for the pipeline
            username: Username for multi-tenant isolation
            project: Project name
            config: Configuration dict with optional "_indexing_pipelines" key

        Returns:
            PipelineSpec for retrieval pipeline
        """
        self.logger.info(
            f"Building retrieval pipeline (async): {pipeline_name} for user: {username}, project: {project}"
        )

        # Step 1: Extract indexing pipeline names
        indexing_pipelines = self._extract_indexing_pipelines(config)

        # Step 2: Fetch embedder and writer components (async)
        indexing_pipeline_components = (
            await self._fetch_indexing_pipeline_components_async(
                indexing_pipelines, username, project
            )
        )

        # Step 3: Build component specs for each pipeline
        retrieval_pipelines_specs = self._build_component_specs(
            component_specs, indexing_pipeline_components
        )

        # Step 4: Build configs for each pipeline
        retrieval_pipelines_configs = self._build_configs(
            config, indexing_pipeline_components
        )

        # Step 5: Build N pipelines using standard indexing builder
        # All branches share same pipeline name but have unique component IDs via branch_id
        built_pipelines = []

        for indexing_pipeline_name in retrieval_pipelines_specs.keys():
            pipeline_spec = retrieval_pipelines_specs[indexing_pipeline_name]
            pipeline_config = retrieval_pipelines_configs[indexing_pipeline_name]

            # Build pipeline using indexing builder with branch_id (async)
            # Set pipeline_type to RETRIEVAL for retrieval branches
            built_pipeline = await self._build_indexing_pipeline_async(
                component_specs=pipeline_spec,
                pipeline_name=pipeline_name,
                username=username,
                project=project,
                config=pipeline_config,
                branch_id=indexing_pipeline_name,
                pipeline_type=PipelineType.RETRIEVAL,
            )

            built_pipeline.indexing_pipelines = [indexing_pipeline_name]
            built_pipelines.append(built_pipeline)

        self.logger.info(
            f"Built {len(built_pipelines)} retrieval pipeline branch(es) (async) for '{pipeline_name}'"
        )

        # Return the first one (they all have the same name)
        if not built_pipelines:
            raise RuntimeError(
                f"Failed to build any retrieval pipeline branches for '{pipeline_name}'"
            )

        return built_pipelines[0]

    def _parse_component_spec(self, spec_item: Dict[str, str]) -> str:
        """
        Parse a component specification dict into a component name.

        Args:
            spec_item: Dict with 'type' key, e.g. {"type": "CONVERTER.PDF"}

        Returns:
            Component registry name

        Raises:
            ValueError: If specification is invalid
        """
        if not isinstance(spec_item, dict):
            raise ValueError(f"Component spec must be dict, got: {type(spec_item)}")

        if "type" not in spec_item:
            raise ValueError("Component spec must have 'type' key")

        type_spec = spec_item["type"]
        if "." not in type_spec:
            raise ValueError(
                f"Component type must be in format 'CATEGORY.TYPE', got: {type_spec}"
            )

        if not validate_component_spec(type_spec):
            raise ValueError(f"Invalid component specification: {type_spec}")

        return get_component_value(type_spec)
