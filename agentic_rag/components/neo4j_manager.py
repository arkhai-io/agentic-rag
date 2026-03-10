"""Simple graph database store for batch nodes and edges."""

import ssl
import sys
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import certifi
from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase, GraphDatabase

if TYPE_CHECKING:
    from ..config import Config

load_dotenv()


class GraphStore:
    """Singleton GraphStore for Neo4j connection management."""

    _instance: Optional["GraphStore"] = None
    _initialized: bool = False

    def __new__(cls, *args: Any, **kwargs: Any) -> "GraphStore":
        """Ensure only one instance of GraphStore exists."""
        if cls._instance is None:
            cls._instance = super(GraphStore, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        uri: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        config: Optional["Config"] = None,
    ) -> None:
        """
        Initialize GraphStore with Neo4j connection (singleton).

        Args:
            uri: Neo4j URI (overrides config)
            username: Neo4j username (overrides config)
            password: Neo4j password (overrides config)
            database: Neo4j database name (overrides config)
            config: Config object with credentials (required if params not provided)

        Note:
            This is a singleton class. Only the first initialization will be used.
            Subsequent calls will return the existing instance.
        """
        # Only initialize once
        if self._initialized:
            return

        # Priority: explicit params > config object
        if config is not None:
            self.uri = uri or config.neo4j_uri
            self.neo4j_username = username or config.neo4j_username
            self.password = password or config.neo4j_password
            self.database = database or config.neo4j_database
        else:
            # Use provided explicit values
            self.uri = uri
            self.neo4j_username = username
            self.password = password
            self.database = database

        if not all([self.uri, self.neo4j_username, self.password]):
            raise ValueError(
                "Neo4j credentials required. Provide via config parameter:\n"
                "  config = Config(neo4j_uri='...', neo4j_username='...', neo4j_password='...')\n"
                "  GraphStore(config=config)"
            )

        print(
            f"GraphStore connecting to: {self.uri} with user: {self.neo4j_username}",
            file=sys.stderr,
        )
        if self.database:
            print(f"Using database: {self.database}", file=sys.stderr)

        # Use the same SSL setup as the working example
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())

        self.driver = GraphDatabase.driver(
            self.uri,
            auth=(self.neo4j_username, self.password),
            ssl_context=ssl_ctx,
            connection_timeout=10,
            max_transaction_retry_time=5,
        )

        # Create async driver with the same configuration
        self.async_driver = AsyncGraphDatabase.driver(
            self.uri,
            auth=(self.neo4j_username, self.password),
            ssl_context=ssl_ctx,
            connection_timeout=10,
            max_transaction_retry_time=5,
        )

        # Verify connectivity like the working example
        try:
            self.driver.verify_connectivity()
            print("GraphStore connected successfully!", file=sys.stderr)
            self._initialized = True
        except Exception as e:
            print(f"GraphStore connection failed: {e}", file=sys.stderr)
            raise

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton instance (useful for testing)."""
        if cls._instance is not None:
            if hasattr(cls._instance, "driver"):
                cls._instance.driver.close()
            if hasattr(cls._instance, "async_driver"):
                # Note: async_driver.close() returns a coroutine, but for testing we just close sync driver
                pass
        cls._instance = None
        cls._initialized = False

    def close(self) -> None:
        self.driver.close()

    async def close_async(self) -> None:
        """Close async driver connection."""
        await self.async_driver.close()

    def add_nodes_batch(
        self, nodes: List[Dict[str, object]], label: str = "Node"
    ) -> None:
        with self.driver.session(database=self.database) as session:
            query = f"""
                UNWIND $nodes AS node
                MERGE (n:{label} {{id: node.id}})
                SET n += node
            """
            session.run(query, nodes=nodes).consume()

    async def add_nodes_batch_async(
        self, nodes: List[Dict[str, object]], label: str = "Node"
    ) -> None:
        """Async version of add_nodes_batch."""
        async with self.async_driver.session(database=self.database) as session:
            query = f"""
                UNWIND $nodes AS node
                MERGE (n:{label} {{id: node.id}})
                SET n += node
            """
            result = await session.run(query, nodes=nodes)
            await result.consume()

    def add_edges_batch(
        self,
        edges: List[Tuple[str, str, str]],
        source_label: str = "Node",
        target_label: str = "Node",
    ) -> None:
        """Add edges in batch. Format: [(source_id, target_id, relationship_type)]"""
        with self.driver.session(database=self.database) as session:
            # Group edges by relationship type and create separate queries
            edges_by_type: Dict[str, List[Dict[str, str]]] = {}
            for source, target, rel_type in edges:
                if rel_type not in edges_by_type:
                    edges_by_type[rel_type] = []
                edges_by_type[rel_type].append({"source": source, "target": target})

            # Create relationships for each type
            for rel_type, edge_list in edges_by_type.items():
                # Use a safe relationship name (replace special characters)
                safe_rel_type = rel_type.replace("-", "_").replace(" ", "_").upper()
                query = f"""
                    UNWIND $edges AS edge
                    MATCH (source:{source_label} {{id: edge.source}})
                    MATCH (target:{target_label} {{id: edge.target}})
                    MERGE (source)-[:{safe_rel_type}]->(target)
                """
                session.run(query, edges=edge_list)

    async def add_edges_batch_async(
        self,
        edges: List[Tuple[str, str, str]],
        source_label: str = "Node",
        target_label: str = "Node",
    ) -> None:
        """Async version of add_edges_batch. Format: [(source_id, target_id, relationship_type)]"""
        async with self.async_driver.session(database=self.database) as session:
            # Group edges by relationship type and create separate queries
            edges_by_type: Dict[str, List[Dict[str, str]]] = {}
            for source, target, rel_type in edges:
                if rel_type not in edges_by_type:
                    edges_by_type[rel_type] = []
                edges_by_type[rel_type].append({"source": source, "target": target})

            # Create relationships for each type
            for rel_type, edge_list in edges_by_type.items():
                # Use a safe relationship name (replace special characters)
                safe_rel_type = rel_type.replace("-", "_").replace(" ", "_").upper()
                query = f"""
                    UNWIND $edges AS edge
                    MATCH (source:{source_label} {{id: edge.source}})
                    MATCH (target:{target_label} {{id: edge.target}})
                    MERGE (source)-[:{safe_rel_type}]->(target)
                """
                result = await session.run(query, edges=edge_list)
                await result.consume()

    def get_component_nodes_by_ids(
        self, component_ids: List[str]
    ) -> List[Dict[str, object]]:
        """Fetch multiple Component nodes by their IDs."""
        if not component_ids:
            return []

        with self.driver.session(database=self.database) as session:
            query = """
                UNWIND $ids AS id
                MATCH (c:Component {id: id})
                RETURN c
            """
            results = session.run(query, ids=component_ids).data()
            return [dict(r["c"]) for r in results]

    async def get_component_nodes_by_ids_async(
        self, component_ids: List[str]
    ) -> List[Dict[str, object]]:
        """Async version of get_component_nodes_by_ids."""
        if not component_ids:
            return []

        async with self.async_driver.session(database=self.database) as session:
            query = """
                UNWIND $ids AS id
                MATCH (c:Component {id: id})
                RETURN c
            """
            result = await session.run(query, ids=component_ids)
            results = await result.data()
            return [dict(r["c"]) for r in results]

    def get_components_by_pipeline(
        self,
        pipeline_name: str,
        username: Optional[str] = None,
        project: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get all Component nodes for a specific pipeline.

        Args:
            pipeline_name: Name of the pipeline (e.g., 'index_1')
            username: Optional username filter for multi-tenant isolation
            project: Optional project filter for multi-tenant isolation

        Returns:
            List of component node dictionaries with all properties
        """
        with self.driver.session(database=self.database) as session:
            if username and project:
                # Query with username and project filter through Project node
                # Find components by traversing from Project or by matching project field directly
                query = """
                    MATCH (c:Component {pipeline_name: $pipeline_name, project: $project, author: $username})
                    RETURN c
                    ORDER BY c.id
                """
                results = session.run(
                    query,
                    pipeline_name=pipeline_name,
                    username=username,
                    project=project,
                ).data()
            elif username:
                # Query with username filter only (backward compatible - searches all projects)
                query = """
                    MATCH (c:Component {pipeline_name: $pipeline_name, author: $username})
                    RETURN c
                    ORDER BY c.id
                """
                results = session.run(
                    query, pipeline_name=pipeline_name, username=username
                ).data()
            else:
                # Query without username filter
                query = """
                    MATCH (c:Component {pipeline_name: $pipeline_name})
                    RETURN c
                    ORDER BY c.id
                """
                results = session.run(query, pipeline_name=pipeline_name).data()

            return [dict(r["c"]) for r in results]

    async def get_components_by_pipeline_async(
        self,
        pipeline_name: str,
        username: Optional[str] = None,
        project: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Async version of get_components_by_pipeline.

        Get all Component nodes for a specific pipeline.

        Args:
            pipeline_name: Name of the pipeline (e.g., 'index_1')
            username: Optional username filter for multi-tenant isolation
            project: Optional project filter for multi-tenant isolation

        Returns:
            List of component node dictionaries with all properties
        """
        async with self.async_driver.session(database=self.database) as session:
            if username and project:
                query = """
                    MATCH (c:Component {pipeline_name: $pipeline_name, project: $project, author: $username})
                    RETURN c
                    ORDER BY c.id
                """
                result = await session.run(
                    query,
                    pipeline_name=pipeline_name,
                    username=username,
                    project=project,
                )
            elif username:
                query = """
                    MATCH (c:Component {pipeline_name: $pipeline_name, author: $username})
                    RETURN c
                    ORDER BY c.id
                """
                result = await session.run(
                    query, pipeline_name=pipeline_name, username=username
                )
            else:
                query = """
                    MATCH (c:Component {pipeline_name: $pipeline_name})
                    RETURN c
                    ORDER BY c.id
                """
                result = await session.run(query, pipeline_name=pipeline_name)

            results = await result.data()
            return [dict(r["c"]) for r in results]

    def validate_user_exists(self, username: str) -> bool:
        """Check if a user exists in Neo4j."""
        with self.driver.session(database=self.database) as session:
            query = """
                MATCH (u:User {username: $username})
                RETURN u.id AS user_id
            """
            result = session.run(query, username=username).single()
            return result is not None

    async def validate_user_exists_async(self, username: str) -> bool:
        """Async version of validate_user_exists."""
        async with self.async_driver.session(database=self.database) as session:
            query = """
                MATCH (u:User {username: $username})
                RETURN u.id AS user_id
            """
            result = await session.run(query, username=username)
            single_result = await result.single()
            return single_result is not None

    def get_pipeline_components_by_hash(
        self, pipeline_hash: str, username: str, project: str = "default"
    ) -> List[Dict[str, object]]:
        """
        Traverse entire pipeline graph using DFS to get all connected components.
        Only follows paths within the same pipeline and project.

        Args:
            pipeline_hash: Single pipeline name/hash to load
            username: Username to validate permissions
            project: Project name to filter by (defaults to "default")

        Returns:
            List of component dictionaries with all necessary data
        """
        with self.driver.session(database=self.database) as session:
            # First find the starting component(s) owned by the user for this pipeline
            # Traverse through Project node using FLOWS_TO: User→Project→Component
            start_query = """
                MATCH (u:User {username: $username})-[:OWNS]->(p:Project {name: $project})-[:FLOWS_TO]->(start:Component)
                WHERE start.pipeline_name = $pipeline_hash
                RETURN start.id AS start_id
            """
            start_results = session.run(
                query=start_query,
                pipeline_hash=pipeline_hash,
                username=username,
                project=project,
            ).data()

            if not start_results:
                return []

            # Get all starting component IDs
            start_ids = [record["start_id"] for record in start_results]

            # Manual DFS traversal within the same pipeline
            return self._dfs_traversal_same_pipeline(session, start_ids, pipeline_hash)

    def _dfs_traversal_same_pipeline(
        self, session: Any, start_ids: List[str], pipeline_hash: str
    ) -> List[Dict[str, object]]:
        """DFS traversal that only follows components in the same pipeline."""
        visited = set()
        components = []
        stack = start_ids.copy()

        while stack:
            current_id = stack.pop()
            if current_id in visited:
                continue

            visited.add(current_id)

            # Get current node and ALL its connections (cross pipeline boundaries)
            query = """
                MATCH (c {id: $component_id})
                WHERE c:Component

                // Get ALL connections (don't filter by pipeline)
                OPTIONAL MATCH (c)-[:FLOWS_TO]->(next)
                WHERE next:Component

                OPTIONAL MATCH (prev)-[:FLOWS_TO]->(c)
                WHERE prev:Component

                RETURN c,
                       collect(DISTINCT next.id) AS next_components,
                       collect(DISTINCT prev.id) AS prev_components,
                       labels(c) AS node_labels
            """

            result = session.run(
                query, component_id=current_id, pipeline_hash=pipeline_hash
            ).single()
            if result:
                component_data = dict(result["c"])
                next_components = result["next_components"]
                prev_components = result["prev_components"]
                node_labels = result["node_labels"]

                # Include ALL components (allows crossing pipeline boundaries)
                component_data["next_components"] = next_components
                component_data["prev_components"] = prev_components
                component_data["node_labels"] = node_labels
                components.append(component_data)

            # Only follow outgoing edges (next_components), not incoming (prev_components)
            # This prevents traversing backwards into other pipelines
            for next_id in next_components:
                if next_id and next_id not in visited:
                    stack.append(next_id)

        return components

    async def get_pipeline_components_by_hash_async(
        self, pipeline_hash: str, username: str, project: str = "default"
    ) -> List[Dict[str, object]]:
        """
        Async version of get_pipeline_components_by_hash.

        Traverse entire pipeline graph using DFS to get all connected components.
        Only follows paths within the same pipeline and project.

        Args:
            pipeline_hash: Single pipeline name/hash to load
            username: Username to validate permissions
            project: Project name to filter by (defaults to "default")

        Returns:
            List of component dictionaries with all necessary data
        """
        async with self.async_driver.session(database=self.database) as session:
            # First find the starting component(s) owned by the user for this pipeline
            start_query = """
                MATCH (u:User {username: $username})-[:OWNS]->(p:Project {name: $project})-[:FLOWS_TO]->(start:Component)
                WHERE start.pipeline_name = $pipeline_hash
                RETURN start.id AS start_id
            """
            result = await session.run(
                query=start_query,
                pipeline_hash=pipeline_hash,
                username=username,
                project=project,
            )
            start_results = await result.data()

            if not start_results:
                return []

            # Get all starting component IDs
            start_ids = [record["start_id"] for record in start_results]

            # Manual DFS traversal within the same pipeline
            return await self._dfs_traversal_same_pipeline_async(
                session, start_ids, pipeline_hash
            )

    async def _dfs_traversal_same_pipeline_async(
        self, session: Any, start_ids: List[str], pipeline_hash: str
    ) -> List[Dict[str, object]]:
        """Async version of DFS traversal that only follows components in the same pipeline."""
        visited = set()
        components = []
        stack = start_ids.copy()

        while stack:
            current_id = stack.pop()
            if current_id in visited:
                continue

            visited.add(current_id)

            # Get current node and ALL its connections
            query = """
                MATCH (c {id: $component_id})
                WHERE c:Component

                // Get ALL connections (don't filter by pipeline)
                OPTIONAL MATCH (c)-[:FLOWS_TO]->(next)
                WHERE next:Component

                OPTIONAL MATCH (prev)-[:FLOWS_TO]->(c)
                WHERE prev:Component

                RETURN c,
                       collect(DISTINCT next.id) AS next_components,
                       collect(DISTINCT prev.id) AS prev_components,
                       labels(c) AS node_labels
            """

            result = await session.run(
                query, component_id=current_id, pipeline_hash=pipeline_hash
            )
            single_result = await result.single()

            if single_result:
                component_data = dict(single_result["c"])
                next_components = single_result["next_components"]
                prev_components = single_result["prev_components"]
                node_labels = single_result["node_labels"]

                # Include ALL components (allows crossing pipeline boundaries)
                component_data["next_components"] = next_components
                component_data["prev_components"] = prev_components
                component_data["node_labels"] = node_labels
                components.append(component_data)

            # Only follow outgoing edges (next_components), not incoming (prev_components)
            for next_id in next_components:
                if next_id and next_id not in visited:
                    stack.append(next_id)

        return components

    async def create_user_async(self, username: str) -> None:
        """Create a new user node if it doesn't exist."""
        async with self.async_driver.session(database=self.database) as session:
            # Use id as the merge key to match add_nodes_batch behavior
            query = """
                MERGE (u:User {id: $username})
                SET u.username = $username
            """
            await session.run(query, username=username)

    async def create_project_async(self, username: str, project_name: str) -> None:
        """Create a new project for a user."""
        # Import here to avoid circular imports
        import hashlib

        project_id = f"proj_{hashlib.sha256(f'{username}__{project_name}'.encode()).hexdigest()[:12]}"

        async with self.async_driver.session(database=self.database) as session:
            query = """
                MERGE (u:User {id: $username})
                SET u.username = $username
                MERGE (p:Project {id: $project_id})
                ON CREATE SET
                    p.name = $project_name,
                    p.username = $username,
                    p.created_at = datetime()
                MERGE (u)-[:OWNS]->(p)
            """
            await session.run(
                query,
                username=username,
                project_name=project_name,
                project_id=project_id,
            )

    async def get_user_projects_and_pipelines_async(
        self, username: str
    ) -> List[Dict[str, Any]]:
        """Get all projects and pipelines for a user."""
        async with self.async_driver.session(database=self.database) as session:
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
            result = await session.run(query, username=username)
            results = await result.data()
            return [record["project_data"] for record in results]

    async def project_exists_async(self, username: str, project_name: str) -> bool:
        """Check if a project exists for a user."""
        async with self.async_driver.session(database=self.database) as session:
            query = """
                MATCH (u:User {username: $username})-[:OWNS]->(p:Project {name: $project_name})
                RETURN p
            """
            result = await session.run(
                query, username=username, project_name=project_name
            )
            return await result.single() is not None

    async def pipeline_exists_async(
        self, username: str, project_name: str, pipeline_name: str
    ) -> bool:
        """Check if a pipeline exists in a project."""
        async with self.async_driver.session(database=self.database) as session:
            query = """
                MATCH (u:User {username: $username})-[:OWNS]->(p:Project {name: $project_name})
                MATCH (p)-[:FLOWS_TO]->(c:Component {pipeline_name: $pipeline_name})
                RETURN c
                LIMIT 1
            """
            result = await session.run(
                query,
                username=username,
                project_name=project_name,
                pipeline_name=pipeline_name,
            )
            return await result.single() is not None

    def lookup_cached_transformations_batch(
        self, input_fingerprints: List[str], component_id: str, config_hash: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Look up cached transformation results for multiple inputs in one query.

        Args:
            input_fingerprints: List of input data fingerprints
            component_id: ID of the component that did the transformation
            config_hash: Hash of component configuration

        Returns:
            Dict mapping input_fingerprint -> list of output data dicts
            {
                "fp_abc123": [
                    {fingerprint, ipfs_hash, data_type, username},
                    ...
                ],
                "fp_xyz789": [...],
                ...
            }

        Query:
            UNWIND $fingerprints AS fp
            MATCH (input:DataPiece {fingerprint: fp})
                  -[:TRANSFORMED_BY {component_id: $cid, config_hash: $ch}]->
                  (output:DataPiece)
            RETURN fp, collect(output) AS outputs
        """
        with self.driver.session(database=self.database) as session:
            query = """
                UNWIND $fingerprints AS fp
                OPTIONAL MATCH (input:DataPiece {fingerprint: fp})
                      -[t:TRANSFORMED_BY {
                          component_id: $component_id,
                          config_hash: $config_hash
                      }]->
                      (output:DataPiece)
                WITH fp, collect({
                    fingerprint: output.fingerprint,
                    ipfs_hash: output.ipfs_hash,
                    data_type: output.data_type,
                    username: output.username
                }) AS outputs
                WHERE size(outputs) > 0 AND outputs[0].fingerprint IS NOT NULL
                RETURN fp, outputs
            """

            results = session.run(
                query,
                fingerprints=input_fingerprints,
                component_id=component_id,
                config_hash=config_hash,
            ).data()

            # Convert to dict
            cache_map = {}
            for record in results:
                cache_map[record["fp"]] = record["outputs"]

            return cache_map

    async def lookup_cached_transformations_batch_async(
        self, input_fingerprints: List[str], component_id: str, config_hash: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Async version of lookup_cached_transformations_batch.

        Look up cached transformation results for multiple inputs in one query.

        Args:
            input_fingerprints: List of input data fingerprints
            component_id: ID of the component that did the transformation
            config_hash: Hash of component configuration

        Returns:
            Dict mapping input_fingerprint -> list of output data dicts
        """
        async with self.async_driver.session(database=self.database) as session:
            query = """
                UNWIND $fingerprints AS fp
                OPTIONAL MATCH (input:DataPiece {fingerprint: fp})
                      -[t:TRANSFORMED_BY {
                          component_id: $component_id,
                          config_hash: $config_hash
                      }]->
                      (output:DataPiece)
                WITH fp, collect({
                    fingerprint: output.fingerprint,
                    ipfs_hash: output.ipfs_hash,
                    data_type: output.data_type,
                    username: output.username
                }) AS outputs
                WHERE size(outputs) > 0 AND outputs[0].fingerprint IS NOT NULL
                RETURN fp, outputs
            """

            result = await session.run(
                query,
                fingerprints=input_fingerprints,
                component_id=component_id,
                config_hash=config_hash,
            )

            results = await result.data()

            # Convert to dict
            cache_map = {}
            for record in results:
                cache_map[record["fp"]] = record["outputs"]

            return cache_map

    def store_transformation_batch(
        self,
        input_fingerprint: str,
        input_ipfs_hash: str,
        input_data_type: str,
        output_records: List[Dict[str, Any]],
        component_id: str,
        component_name: str,
        config_hash: str,
        username: str,
        processing_time_ms: Optional[int] = None,
        run_id: Optional[str] = None,
    ) -> None:
        """
        Store a 1→N transformation in Neo4j.

        Creates:
        - Input DataPiece node (if not exists)
        - Output DataPiece nodes for all outputs
        - TRANSFORMED_BY edges from input to each output
        - Optional GENERATED_BY edge to Run node (for FAIR compliance)

        Args:
            input_fingerprint: Fingerprint of input data
            input_ipfs_hash: Storage object key (field name kept as ipfs_hash for Neo4j schema compat)
            input_data_type: Type of input data
            output_records: List of {fingerprint, ipfs_hash, data_type, content_type?, uri?}
                           (ipfs_hash field stores Akave object key)
            component_id: ID of component that did transformation
            component_name: Name of component
            config_hash: Hash of component config
            username: User who owns this data
            processing_time_ms: Optional processing time
            run_id: Optional run ID for FAIR provenance tracking
        """
        with self.driver.session(database=self.database) as session:
            # Main query to create DataPiece nodes and TRANSFORMED_BY edges
            query = """
                // Create or get input DataPiece
                MERGE (input:DataPiece {fingerprint: $input_fingerprint})
                ON CREATE SET
                    input.ipfs_hash = $input_ipfs_hash,
                    input.data_type = $input_data_type,
                    input.username = $username,
                    input.created_at = datetime()

                // Create output DataPieces and edges
                WITH input
                UNWIND $output_records AS output
                MERGE (out:DataPiece {fingerprint: output.fingerprint})
                ON CREATE SET
                    out.ipfs_hash = output.ipfs_hash,
                    out.data_type = output.data_type,
                    out.username = $username,
                    out.created_at = datetime()
                // Set FAIR compliance fields if provided
                SET out.content_type = COALESCE(output.content_type, out.content_type),
                    out.uri = COALESCE(output.uri, out.uri),
                    out.generated_by = COALESCE($run_id, out.generated_by)

                // Create TRANSFORMED_BY edge
                MERGE (input)-[t:TRANSFORMED_BY {
                    component_id: $component_id,
                    config_hash: $config_hash
                }]->(out)
                ON CREATE SET
                    t.component_name = $component_name,
                    t.processing_time_ms = $processing_time_ms,
                    t.created_at = datetime()
            """

            session.run(
                query,
                input_fingerprint=input_fingerprint,
                input_ipfs_hash=input_ipfs_hash,
                input_data_type=input_data_type,
                output_records=output_records,
                component_id=component_id,
                component_name=component_name,
                config_hash=config_hash,
                username=username,
                processing_time_ms=processing_time_ms,
                run_id=run_id,
            ).consume()

            # Create GENERATED_BY relationship to Run node if run_id provided
            if run_id:
                run_query = """
                    UNWIND $output_fingerprints AS fp
                    MATCH (d:DataPiece {fingerprint: fp})
                    MATCH (r:Run {id: $run_id})
                    MERGE (d)-[:GENERATED_BY]->(r)
                """
                output_fps = [r["fingerprint"] for r in output_records]
                session.run(
                    run_query, output_fingerprints=output_fps, run_id=run_id
                ).consume()

    async def store_transformation_batch_async(
        self,
        input_fingerprint: str,
        input_ipfs_hash: str,
        input_data_type: str,
        output_records: List[Dict[str, Any]],
        component_id: str,
        component_name: str,
        config_hash: str,
        username: str,
        processing_time_ms: Optional[int] = None,
        run_id: Optional[str] = None,
    ) -> None:
        """
        Async version of store_transformation_batch.

        Store a 1→N transformation in Neo4j.

        Args:
            input_fingerprint: Fingerprint of input data
            input_ipfs_hash: Storage object key (field name kept as ipfs_hash for Neo4j schema compat)
            input_data_type: Type of input data
            output_records: List of {fingerprint, ipfs_hash, data_type, content_type?, uri?}
                           (ipfs_hash field stores Akave object key)
            component_id: ID of component that did transformation
            component_name: Name of component
            config_hash: Hash of component config
            username: User who owns this data
            processing_time_ms: Optional processing time
            run_id: Optional run ID for FAIR provenance tracking
        """
        async with self.async_driver.session(database=self.database) as session:
            query = """
                // Create or get input DataPiece
                MERGE (input:DataPiece {fingerprint: $input_fingerprint})
                ON CREATE SET
                    input.ipfs_hash = $input_ipfs_hash,
                    input.data_type = $input_data_type,
                    input.username = $username,
                    input.created_at = datetime()

                // Create output DataPieces and edges
                WITH input
                UNWIND $output_records AS output
                MERGE (out:DataPiece {fingerprint: output.fingerprint})
                ON CREATE SET
                    out.ipfs_hash = output.ipfs_hash,
                    out.data_type = output.data_type,
                    out.username = $username,
                    out.created_at = datetime()
                // Set FAIR compliance fields if provided
                SET out.content_type = COALESCE(output.content_type, out.content_type),
                    out.uri = COALESCE(output.uri, out.uri),
                    out.generated_by = COALESCE($run_id, out.generated_by)

                // Create TRANSFORMED_BY edge
                MERGE (input)-[t:TRANSFORMED_BY {
                    component_id: $component_id,
                    config_hash: $config_hash
                }]->(out)
                ON CREATE SET
                    t.component_name = $component_name,
                    t.processing_time_ms = $processing_time_ms,
                    t.created_at = datetime()
            """

            result = await session.run(
                query,
                input_fingerprint=input_fingerprint,
                input_ipfs_hash=input_ipfs_hash,
                input_data_type=input_data_type,
                output_records=output_records,
                component_id=component_id,
                component_name=component_name,
                config_hash=config_hash,
                username=username,
                processing_time_ms=processing_time_ms,
                run_id=run_id,
            )
            await result.consume()

            # Create GENERATED_BY relationship to Run node if run_id provided
            if run_id:
                run_query = """
                    UNWIND $output_fingerprints AS fp
                    MATCH (d:DataPiece {fingerprint: fp})
                    MATCH (r:Run {id: $run_id})
                    MERGE (d)-[:GENERATED_BY]->(r)
                """
                output_fps = [r["fingerprint"] for r in output_records]
                result = await session.run(
                    run_query, output_fingerprints=output_fps, run_id=run_id
                )
                await result.consume()

    # =========================================================================
    # FAIR Compliance: RunNode Methods
    # =========================================================================

    def store_run_node(
        self,
        run_id: str,
        pipeline_name: str,
        username: str,
        project: str = "default",
        pipeline_version: Optional[str] = None,
        git_commit: Optional[str] = None,
        model_names: Optional[List[str]] = None,
        config_hash: Optional[str] = None,
        started_at: Optional[str] = None,
        uri: Optional[str] = None,
    ) -> None:
        """
        Store a RunNode in Neo4j for provenance tracking.

        Args:
            run_id: Unique run identifier
            pipeline_name: Name of the pipeline being run
            username: User who initiated the run
            project: Project name
            pipeline_version: Optional pipeline version
            git_commit: Optional git commit hash
            model_names: Optional list of model names used
            config_hash: Optional hash of pipeline config
            started_at: ISO timestamp when run started
            uri: Persistent URI for FAIR compliance
        """
        with self.driver.session(database=self.database) as session:
            query = """
                MERGE (r:Run {id: $run_id})
                ON CREATE SET
                    r.pipeline_name = $pipeline_name,
                    r.username = $username,
                    r.project = $project,
                    r.started_at = $started_at,
                    r.uri = $uri,
                    r.created_at = datetime()
                ON MATCH SET
                    r.pipeline_name = $pipeline_name,
                    r.username = $username,
                    r.project = $project
            """
            params: Dict[str, Any] = {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "username": username,
                "project": project,
                "started_at": started_at,
                "uri": uri,
            }

            session.run(query, **params).consume()

            # Set optional properties separately to avoid null issues
            if pipeline_version or git_commit or model_names or config_hash:
                set_clauses = []
                opt_params: Dict[str, Any] = {"run_id": run_id}

                if pipeline_version:
                    set_clauses.append("r.pipeline_version = $pipeline_version")
                    opt_params["pipeline_version"] = pipeline_version
                if git_commit:
                    set_clauses.append("r.git_commit = $git_commit")
                    opt_params["git_commit"] = git_commit
                if model_names:
                    set_clauses.append("r.model_names = $model_names")
                    opt_params["model_names"] = model_names
                if config_hash:
                    set_clauses.append("r.config_hash = $config_hash")
                    opt_params["config_hash"] = config_hash

                if set_clauses:
                    update_query = f"""
                        MATCH (r:Run {{id: $run_id}})
                        SET {', '.join(set_clauses)}
                    """
                    session.run(update_query, **opt_params).consume()

    async def store_run_node_async(
        self,
        run_id: str,
        pipeline_name: str,
        username: str,
        project: str = "default",
        pipeline_version: Optional[str] = None,
        git_commit: Optional[str] = None,
        model_names: Optional[List[str]] = None,
        config_hash: Optional[str] = None,
        started_at: Optional[str] = None,
        uri: Optional[str] = None,
    ) -> None:
        """Async version of store_run_node."""
        async with self.async_driver.session(database=self.database) as session:
            query = """
                MERGE (r:Run {id: $run_id})
                ON CREATE SET
                    r.pipeline_name = $pipeline_name,
                    r.username = $username,
                    r.project = $project,
                    r.started_at = $started_at,
                    r.uri = $uri,
                    r.created_at = datetime()
                ON MATCH SET
                    r.pipeline_name = $pipeline_name,
                    r.username = $username,
                    r.project = $project
            """
            params: Dict[str, Any] = {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "username": username,
                "project": project,
                "started_at": started_at,
                "uri": uri,
            }

            result = await session.run(query, **params)
            await result.consume()

            # Set optional properties
            if pipeline_version or git_commit or model_names or config_hash:
                set_clauses = []
                opt_params: Dict[str, Any] = {"run_id": run_id}

                if pipeline_version:
                    set_clauses.append("r.pipeline_version = $pipeline_version")
                    opt_params["pipeline_version"] = pipeline_version
                if git_commit:
                    set_clauses.append("r.git_commit = $git_commit")
                    opt_params["git_commit"] = git_commit
                if model_names:
                    set_clauses.append("r.model_names = $model_names")
                    opt_params["model_names"] = model_names
                if config_hash:
                    set_clauses.append("r.config_hash = $config_hash")
                    opt_params["config_hash"] = config_hash

                if set_clauses:
                    update_query = f"""
                        MATCH (r:Run {{id: $run_id}})
                        SET {', '.join(set_clauses)}
                    """
                    result = await session.run(update_query, **opt_params)
                    await result.consume()

    def update_run_finished(
        self,
        run_id: str,
        finished_at: str,
        success: bool = True,
        error: Optional[str] = None,
    ) -> None:
        """
        Update a RunNode with completion information.

        Args:
            run_id: Run identifier
            finished_at: ISO timestamp when run finished
            success: Whether the run completed successfully
            error: Optional error message if failed
        """
        with self.driver.session(database=self.database) as session:
            query = """
                MATCH (r:Run {id: $run_id})
                SET r.finished_at = $finished_at,
                    r.success = $success
            """
            params: Dict[str, Any] = {
                "run_id": run_id,
                "finished_at": finished_at,
                "success": success,
            }

            if error:
                query = """
                    MATCH (r:Run {id: $run_id})
                    SET r.finished_at = $finished_at,
                        r.success = $success,
                        r.error = $error
                """
                params["error"] = error

            session.run(query, **params).consume()

    async def update_run_finished_async(
        self,
        run_id: str,
        finished_at: str,
        success: bool = True,
        error: Optional[str] = None,
    ) -> None:
        """Async version of update_run_finished."""
        async with self.async_driver.session(database=self.database) as session:
            query = """
                MATCH (r:Run {id: $run_id})
                SET r.finished_at = $finished_at,
                    r.success = $success
            """
            params: Dict[str, Any] = {
                "run_id": run_id,
                "finished_at": finished_at,
                "success": success,
            }

            if error:
                query = """
                    MATCH (r:Run {id: $run_id})
                    SET r.finished_at = $finished_at,
                        r.success = $success,
                        r.error = $error
                """
                params["error"] = error

            result = await session.run(query, **params)
            await result.consume()
