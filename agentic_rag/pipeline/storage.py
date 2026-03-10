"""Graph storage for creating and managing graph representations of pipelines."""

import sys
from typing import Any, Dict, List, Optional, Tuple

from haystack import Pipeline

from ..components import ComponentRegistry, GraphStore
from ..types import (
    ComponentNode,
    ComponentSpec,
    GraphRelationship,
    PipelineSpec,
    PipelineType,
    ProjectNode,
    UserNode,
    create_haystack_component,
)
from ..utils.logger import get_system_logger


class GraphStorage:
    """Stores and manages graph representations of pipeline components and their relationships (Singleton)."""

    _instance: Optional["GraphStorage"] = None
    _initialized: bool = False

    def __new__(cls, *args: Any, **kwargs: Any) -> "GraphStorage":
        """Ensure only one instance of GraphStorage exists."""
        if cls._instance is None:
            cls._instance = super(GraphStorage, cls).__new__(cls)
        return cls._instance

    def __init__(self, graph_store: GraphStore, registry: ComponentRegistry) -> None:
        """
        Initialize GraphStorage (singleton).

        Args:
            graph_store: GraphStore instance for Neo4j operations
            registry: ComponentRegistry for component specifications

        Note:
            This is a singleton class. Only the first initialization will be used.
            Subsequent calls will return the existing instance.
        """
        # Only initialize once
        if self._initialized:
            return

        self.graph_store = graph_store
        self.registry = registry
        self.logger = get_system_logger(__name__)
        self._initialized = True

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton instance (useful for testing)."""
        cls._instance = None
        cls._initialized = False

    def create_pipeline_graph(
        self,
        spec: PipelineSpec,
        connections: List[Tuple[str, str]],
        username: str,
        project: str = "default",
        branch_id: Optional[str] = None,
    ) -> None:
        """Create graph representation of the pipeline components.

        Args:
            spec: Pipeline specification
            connections: List of (source, target) component connections
            username: Username for pipeline ownership
            project: Project name (defaults to "default")
            branch_id: Optional branch identifier for retrieval pipeline branches
        """

        nodes: List[Dict[str, Any]] = []
        node_id_by_name: Dict[str, str] = {}
        for component_spec in spec.components:
            node = ComponentNode(
                component_name=component_spec.name,
                pipeline_name=spec.name,
                project=project,
                version="1.0.0",
                author=username,
                component_config=component_spec.get_config(),
                component_type=(
                    component_spec.full_type if component_spec.full_type else None
                ),
                pipeline_type=spec.pipeline_type.value if spec.pipeline_type else None,
                branch_id=branch_id,
            )
            node_dict = node.to_dict()
            nodes.append(node_dict)
            node_id_by_name[component_spec.name] = node_dict["id"]

        # Add/update the owning user node
        self.logger.info(
            f"Creating pipeline graph for user '{username}', project '{project}', pipeline '{spec.name}'"
        )
        user_node = UserNode(username=username, display_name=username)
        user_dict = user_node.to_dict()
        self.graph_store.add_nodes_batch([user_dict], "User")

        # Add/update the project node
        project_node = ProjectNode(name=project, username=username)
        project_dict = project_node.to_dict()
        self.graph_store.add_nodes_batch([project_dict], "Project")

        # Connect user to project (User -[:OWNS]-> Project)
        self.graph_store.add_edges_batch(
            [
                (
                    user_dict["id"],
                    project_dict["id"],
                    GraphRelationship.OWNS.value,
                )
            ],
            source_label="User",
            target_label="Project",
        )

        # Add component nodes
        self.logger.debug(f"Adding {len(nodes)} component nodes to graph")
        self.graph_store.add_nodes_batch(nodes, "Component")

        # Connect project to the first component (Project -[:FLOWS_TO]-> Component)
        if spec.components:
            first_component_id = node_id_by_name.get(spec.components[0].name)
            if first_component_id:
                self.graph_store.add_edges_batch(
                    [
                        (
                            project_dict["id"],
                            first_component_id,
                            GraphRelationship.FLOWS_TO.value,
                        )
                    ],
                    source_label="Project",
                    target_label="Component",
                )

        # Connect sequential components
        if connections:
            component_edges = []
            for source_name, target_name in connections:
                source_id = node_id_by_name.get(source_name)
                target_id = node_id_by_name.get(target_name)
                if source_id and target_id:
                    component_edges.append(
                        (source_id, target_id, GraphRelationship.FLOWS_TO.value)
                    )
            if component_edges:
                self.graph_store.add_edges_batch(
                    component_edges,
                    source_label="Component",
                    target_label="Component",
                )

        # Retrieval pipelines will be handled separately
        # No need to store graph for retrieval - they're built dynamically from indexing metadata
        if spec.pipeline_type == PipelineType.RETRIEVAL:
            self.logger.info(
                f"Retrieval pipeline '{spec.name}' - will be built dynamically from indexing pipeline metadata"
            )

    async def create_pipeline_graph_async(
        self,
        spec: PipelineSpec,
        connections: List[Tuple[str, str]],
        username: str,
        project: str = "default",
        branch_id: Optional[str] = None,
    ) -> None:
        """Async version of create_pipeline_graph.

        Create graph representation of the pipeline components.

        Args:
            spec: Pipeline specification
            connections: List of (source, target) component connections
            username: Username for pipeline ownership
            project: Project name (defaults to "default")
            branch_id: Optional branch identifier for retrieval pipeline branches
        """

        nodes: List[Dict[str, Any]] = []
        node_id_by_name: Dict[str, str] = {}
        for component_spec in spec.components:
            node = ComponentNode(
                component_name=component_spec.name,
                pipeline_name=spec.name,
                project=project,
                version="1.0.0",
                author=username,
                component_config=component_spec.get_config(),
                component_type=(
                    component_spec.full_type if component_spec.full_type else None
                ),
                pipeline_type=spec.pipeline_type.value if spec.pipeline_type else None,
                branch_id=branch_id,
            )
            node_dict = node.to_dict()
            nodes.append(node_dict)
            node_id_by_name[component_spec.name] = node_dict["id"]

        # Add/update the owning user node
        self.logger.info(
            f"Creating pipeline graph (async) for user '{username}', project '{project}', pipeline '{spec.name}'"
        )
        user_node = UserNode(username=username, display_name=username)
        user_dict = user_node.to_dict()
        await self.graph_store.add_nodes_batch_async([user_dict], "User")

        # Add/update the project node
        project_node = ProjectNode(name=project, username=username)
        project_dict = project_node.to_dict()
        await self.graph_store.add_nodes_batch_async([project_dict], "Project")

        # Connect user to project (User -[:OWNS]-> Project)
        await self.graph_store.add_edges_batch_async(
            [
                (
                    user_dict["id"],
                    project_dict["id"],
                    GraphRelationship.OWNS.value,
                )
            ],
            source_label="User",
            target_label="Project",
        )

        # Add component nodes
        self.logger.debug(f"Adding {len(nodes)} component nodes to graph")
        await self.graph_store.add_nodes_batch_async(nodes, "Component")

        # Connect project to the first component (Project -[:FLOWS_TO]-> Component)
        if spec.components:
            first_component_id = node_id_by_name.get(spec.components[0].name)
            if first_component_id:
                await self.graph_store.add_edges_batch_async(
                    [
                        (
                            project_dict["id"],
                            first_component_id,
                            GraphRelationship.FLOWS_TO.value,
                        )
                    ],
                    source_label="Project",
                    target_label="Component",
                )

        # Connect sequential components
        if connections:
            component_edges = []
            for source_name, target_name in connections:
                source_id = node_id_by_name.get(source_name)
                target_id = node_id_by_name.get(target_name)
                if source_id and target_id:
                    component_edges.append(
                        (source_id, target_id, GraphRelationship.FLOWS_TO.value)
                    )
            if component_edges:
                await self.graph_store.add_edges_batch_async(
                    component_edges,
                    source_label="Component",
                    target_label="Component",
                )

        # Retrieval pipelines will be handled separately
        if spec.pipeline_type == PipelineType.RETRIEVAL:
            self.logger.info(
                f"Retrieval pipeline '{spec.name}' - will be built dynamically from indexing pipeline metadata"
            )

    def build_pipeline_graph(
        self,
        spec: PipelineSpec,
        username: str = "test_user",
        project: str = "default",
        branch_id: Optional[str] = None,
    ) -> None:
        """Build a graph representation of the pipeline specification.

        Args:
            spec: Pipeline specification
            username: Username for pipeline ownership (defaults to "test_user")
            project: Project name (defaults to "default")
            branch_id: Optional branch identifier for retrieval pipeline branches
        """

        # Determine connections based on component order and dependencies
        connections = self._determine_connections(spec.components)

        # Create graph representation
        self.create_pipeline_graph(spec, connections, username, project, branch_id)

    async def build_pipeline_graph_async(
        self,
        spec: PipelineSpec,
        username: str = "test_user",
        project: str = "default",
        branch_id: Optional[str] = None,
    ) -> None:
        """Async version of build_pipeline_graph.

        Build a graph representation of the pipeline specification.

        Args:
            spec: Pipeline specification
            username: Username for pipeline ownership (defaults to "test_user")
            project: Project name (defaults to "default")
            branch_id: Optional branch identifier for retrieval pipeline branches
        """

        # Determine connections based on component order and dependencies
        connections = self._determine_connections(spec.components)

        # Create graph representation
        await self.create_pipeline_graph_async(
            spec, connections, username, project, branch_id
        )

    def build_haystack_pipeline(self, spec: PipelineSpec) -> Any:
        """Build a Haystack pipeline from a pipeline specification."""

        # Create Haystack pipeline
        pipeline = Pipeline()

        # Create component instances and add to pipeline
        for component_spec in spec.components:
            # ComponentSpec handles its own config internally
            haystack_component = create_haystack_component(component_spec)

            pipeline.add_component(component_spec.name, haystack_component)

        # Determine connections based on component order and dependencies
        connections = self._determine_connections(spec.components)

        # Add connections to pipeline
        for source, target in connections:
            try:
                pipeline.connect(source, target)
            except Exception as e:
                print(
                    f"Warning: Could not connect {source} -> {target}: {e}",
                    file=sys.stderr,
                )

        return pipeline

    def _determine_connections(
        self, components: List[ComponentSpec]
    ) -> List[Tuple[str, str]]:
        """Determine how components should be connected - simple sequential connections."""
        connections: List[Tuple[str, str]] = []

        # Connect each component to the previous one
        for i in range(1, len(components)):
            prev_component = components[i - 1]
            current_component = components[i]
            connections.append((prev_component.name, current_component.name))

        return connections

    def load_pipeline_by_hashes(
        self, pipeline_hashes: List[str], username: str, project: str = "default"
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Retrieve components for each pipeline hash from Neo4j.

        Args:
            pipeline_hashes: List of pipeline names to load
            username: Username for permissions
            project: Project name to filter by (defaults to "default")

        Returns:
            Dictionary mapping pipeline names to their component data
        """
        # 1. Validate username exists in Neo4j
        if not self.graph_store.validate_user_exists(username):
            raise ValueError(f"Username '{username}' not found in Neo4j")

        pipelines_data: Dict[str, List[Dict[str, Any]]] = {}

        # 2. Fetch components for each pipeline hash separately
        for pipeline_hash in pipeline_hashes:
            print(
                f"\nFetching components for pipeline: {pipeline_hash}",
                file=sys.stderr,
            )

            # Call Neo4j for this specific pipeline hash (single hash method)
            component_data_list = self.graph_store.get_pipeline_components_by_hash(
                pipeline_hash,
                username,
                project,  # Single pipeline hash with project filter
            )

            print(f"   Found {len(component_data_list)} components", file=sys.stderr)

            if component_data_list:
                pipelines_data[pipeline_hash] = component_data_list

                # Print details for this pipeline
                print(component_data_list, file=sys.stderr)

        return pipelines_data

    async def load_pipeline_by_hashes_async(
        self, pipeline_hashes: List[str], username: str, project: str = "default"
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Async version of load_pipeline_by_hashes.

        Retrieve components for each pipeline hash from Neo4j.

        Args:
            pipeline_hashes: List of pipeline names to load
            username: Username for permissions
            project: Project name to filter by (defaults to "default")

        Returns:
            Dictionary mapping pipeline names to their component data
        """
        # 1. Validate username exists in Neo4j (async)
        if not await self.graph_store.validate_user_exists_async(username):
            raise ValueError(f"Username '{username}' not found in Neo4j")

        pipelines_data: Dict[str, List[Dict[str, Any]]] = {}

        # 2. Fetch components for each pipeline hash separately
        for pipeline_hash in pipeline_hashes:
            print(
                f"\nFetching components for pipeline (async): {pipeline_hash}",
                file=sys.stderr,
            )

            # Call Neo4j for this specific pipeline hash (async)
            component_data_list = (
                await self.graph_store.get_pipeline_components_by_hash_async(
                    pipeline_hash,
                    username,
                    project,
                )
            )

            print(f"   Found {len(component_data_list)} components", file=sys.stderr)

            if component_data_list:
                pipelines_data[pipeline_hash] = component_data_list

        return pipelines_data
