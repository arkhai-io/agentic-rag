"""Generic wrapper that adds caching gates to any Haystack component.

FAIR Compliance:
- Supports content_type from ComponentSpec registry
- Supports run_id for provenance tracking
- Passes FAIR fields through to OutGate for storage
"""

import asyncio
import contextvars
import time
from typing import Any, Dict, List, Optional

from ...types.node_types import ContentType
from ...utils.logger import get_logger
from ...utils.metrics import MetricsCollector
from ..neo4j_manager import GraphStore
from .ingate import InGate
from .outgate import OutGate


def get_content_type_from_registry(component_name: str) -> Optional[ContentType]:
    """
    Get the FAIR content_type from the component registry.

    Args:
        component_name: Name of the component (e.g., "markdown_aware_chunker")

    Returns:
        ContentType enum value or None if not found/specified
    """
    from ..registry import get_default_registry

    registry = get_default_registry()
    spec = registry.get_component_spec(component_name)

    if spec and spec.produces_content_type:
        try:
            return ContentType(spec.produces_content_type)
        except ValueError:
            pass

    # Fallback to None if not in registry or not specified
    return None


class GatedComponent:
    """
    Generic wrapper that adds InGate/OutGate caching to any component.

    Wraps any Haystack component and automatically:
    1. Checks cache before processing (InGate)
    2. Runs component only on uncached items
    3. Stores results to cache (OutGate)
    4. Merges cached + new results

    Supports both synchronous and asynchronous component execution.
    If the wrapped component has a `run_async` method, it will be used for async calls.
    Otherwise, the sync `run` method will be executed in a thread pool.

    Example:
        >>> chunker = MarkdownAwareChunker(chunk_size=500)
        >>> gated_chunker = GatedComponent(
        ...     component=chunker,
        ...     component_id="chunker_1",
        ...     component_name="markdown_chunker",
        ...     graph_store=graph_store,
        ...     username="alice"
        ... )
        >>>
        >>> # Use like normal component (sync)
        >>> result = gated_chunker.run(documents=[doc1, doc2, doc3])
        >>> # Automatically cached!
        >>>
        >>> # Or use async
        >>> result = await gated_chunker.run_async(documents=[doc1, doc2, doc3])
    """

    def __init__(
        self,
        component: Any,
        component_id: str,
        component_name: str,
        graph_store: GraphStore,
        username: str,
        cache_key: Optional[str] = None,
        retrieve_from_storage: bool = True,
        run_id: Optional[str] = None,
        content_type: Optional[ContentType] = None,
    ):
        """
        Initialize gated component wrapper.

        Args:
            component: The Haystack component to wrap
            component_id: Unique ID for this component instance
            component_name: Human-readable name
            graph_store: Neo4j graph store
            username: User who owns this pipeline/data
            cache_key: Pipeline-agnostic cache key for cache lookups
            retrieve_from_storage: If True, retrieves cached data from Akave (default: False)
            run_id: FAIR provenance - Run ID to link all outputs to
            content_type: FAIR semantic type (auto-inferred from component_name if not provided)
        """
        self.component = component
        self.component_id = component_id
        self.component_name = component_name
        self.graph_store = graph_store
        self.username = username
        self.cache_key = (
            cache_key or component_id
        )  # Fall back to component_id if no cache_key
        self.logger = get_logger(f"{__name__}.{component_name}", username=username)
        self.metrics = MetricsCollector(username=username)

        # FAIR compliance fields
        self.run_id = run_id
        # Get content_type from registry if not provided
        self.content_type = content_type or get_content_type_from_registry(
            component_name
        )

        # Create gates with storage retrieval enabled (use cache_key for lookups)
        self.ingate = InGate(
            graph_store=graph_store,
            component_id=self.cache_key,  # Use cache_key for lookups
            component_name=component_name,
            username=username,
            retrieve_from_storage=True,  # Always retrieve from Akave for cached results
        )

        self.outgate = OutGate(
            graph_store=graph_store,
            component_id=self.cache_key,  # Use cache_key for storage
            component_name=component_name,
            username=username,
        )

    def set_run_id(self, run_id: str) -> None:
        """
        Set the run_id for FAIR provenance tracking.

        This can be called after construction to link outputs to a specific Run.

        Args:
            run_id: The Run ID to link all outputs to
        """
        self.run_id = run_id

    def run(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Run component with caching.

        Flow:
        1. Check cache with InGate
        2. If all inputs cached → skip component, return cached metadata
        3. If some/all uncached → run component on uncached, store results
        4. Return component output

        Args:
            **kwargs: Component inputs (e.g., documents=[...], query="...", etc.)

        Returns:
            Component output
        """
        start_time = time.time()

        # Extract component config
        component_config = self._extract_component_config()

        # Find the main input parameter
        input_param, input_items = self._extract_input_data(kwargs)

        # If no cacheable inputs, just run normally
        if not input_items:
            result: Dict[str, Any] = self.component.run(**kwargs)
            return result

        # Check cache with InGate
        cache_result = self.ingate.check_cache_batch(
            input_items=input_items,
            component_config=component_config,
        )

        cached_items = cache_result["cached"]
        uncached_items = cache_result["uncached"]
        cache_hits = len(cached_items)
        cache_misses = len(uncached_items)

        # If everything is cached, skip component execution
        if not uncached_items:
            end_time = time.time()
            self.metrics.log_component_execution(
                component_name=self.component_name,
                component_id=self.component_id,
                start_time=start_time,
                end_time=end_time,
                input_count=len(input_items),
                output_count=len(cached_items),
                cache_hits=cache_hits,
                cache_misses=cache_misses,
            )
            return self._format_cached_output(cached_items)

        # Run component on uncached items only
        if len(uncached_items) < len(input_items) and input_param is not None:
            # Partial cache - run only on uncached
            kwargs_uncached = kwargs.copy()
            kwargs_uncached[input_param] = uncached_items
            component_output = self.component.run(**kwargs_uncached)
        else:
            # No cache - run on all
            component_output = self.component.run(**kwargs)

        # Extract outputs
        output_items = self._extract_output_data(component_output)

        # Store new results to cache - process each item individually to maintain correct mappings
        if uncached_items:
            for input_item in uncached_items:
                try:
                    # Run component on single item to get its specific outputs
                    kwargs_single = kwargs.copy()
                    if input_param:
                        kwargs_single[input_param] = [input_item]
                    single_output = self.component.run(**kwargs_single)
                    single_output_items = self._extract_output_data(single_output)

                    # Store: input → its outputs (handles both 1→1 and 1→N)
                    # Include FAIR compliance fields
                    if single_output_items:
                        self.outgate.store(
                            input_data=input_item,
                            output_data=single_output_items,
                            component_config=component_config,
                            content_type=self.content_type,
                            run_id=self.run_id,
                        )
                except Exception as e:
                    self.logger.warning(f"Failed to cache item: {e}")
                    pass  # Continue with next item

        end_time = time.time()

        # Log metrics
        self.metrics.log_component_execution(
            component_name=self.component_name,
            component_id=self.component_id,
            start_time=start_time,
            end_time=end_time,
            input_count=len(input_items),
            output_count=len(output_items),
            cache_hits=cache_hits,
            cache_misses=cache_misses,
        )

        # Return component output
        typed_output: Dict[str, Any] = component_output
        return typed_output

    async def run_async(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Asynchronously run component with caching.

        This is the async version of the `run` method with the same caching flow:
        1. Check cache with InGate
        2. If all inputs cached → skip component, return cached metadata
        3. If some/all uncached → run component on uncached, store results
        4. Return component output

        Args:
            **kwargs: Component inputs (e.g., documents=[...], query="...", etc.)

        Returns:
            Component output
        """
        start_time = time.time()

        # Extract component config
        component_config = self._extract_component_config()

        # Find the main input parameter
        input_param, input_items = self._extract_input_data(kwargs)

        # If no cacheable inputs, just run normally
        if not input_items:
            # Check if component supports async (Haystack pattern)
            if getattr(self.component, "__haystack_supports_async__", False):
                result: Dict[str, Any] = await self.component.run_async(**kwargs)
            else:
                # Fallback to sync run in executor with context preservation
                loop = asyncio.get_running_loop()
                ctx = contextvars.copy_context()
                result = await loop.run_in_executor(
                    None, lambda: ctx.run(lambda: self.component.run(**kwargs))
                )
            return result

        # Check cache with InGate (async operation)
        cache_result = await self.ingate.check_cache_batch_async(
            input_items=input_items,
            component_config=component_config,
        )

        cached_items = cache_result["cached"]
        uncached_items = cache_result["uncached"]
        cache_hits = len(cached_items)
        cache_misses = len(uncached_items)

        # If everything is cached, skip component execution
        if not uncached_items:
            end_time = time.time()
            self.metrics.log_component_execution(
                component_name=self.component_name,
                component_id=self.component_id,
                start_time=start_time,
                end_time=end_time,
                input_count=len(input_items),
                output_count=len(cached_items),
                cache_hits=cache_hits,
                cache_misses=cache_misses,
            )
            return self._format_cached_output(cached_items)

        # Run component on uncached items only
        if len(uncached_items) < len(input_items) and input_param is not None:
            # Partial cache - run only on uncached
            kwargs_uncached = kwargs.copy()
            kwargs_uncached[input_param] = uncached_items
            if getattr(self.component, "__haystack_supports_async__", False):
                component_output = await self.component.run_async(**kwargs_uncached)
            else:
                loop = asyncio.get_running_loop()
                ctx = contextvars.copy_context()
                component_output = await loop.run_in_executor(
                    None, lambda: ctx.run(lambda: self.component.run(**kwargs_uncached))
                )
        else:
            # No cache - run on all
            if getattr(self.component, "__haystack_supports_async__", False):
                component_output = await self.component.run_async(**kwargs)
            else:
                loop = asyncio.get_running_loop()
                ctx = contextvars.copy_context()
                component_output = await loop.run_in_executor(
                    None, lambda: ctx.run(lambda: self.component.run(**kwargs))
                )

        # Extract outputs
        output_items = self._extract_output_data(component_output)

        # Store new results to cache - process each item individually to maintain correct mappings
        if uncached_items:
            for input_item in uncached_items:
                try:
                    # Run component on single item to get its specific outputs
                    kwargs_single = kwargs.copy()
                    if input_param:
                        kwargs_single[input_param] = [input_item]

                    if getattr(self.component, "__haystack_supports_async__", False):
                        single_output = await self.component.run_async(**kwargs_single)
                    else:
                        loop = asyncio.get_running_loop()
                        ctx = contextvars.copy_context()
                        single_output = await loop.run_in_executor(
                            None,
                            lambda: ctx.run(
                                lambda: self.component.run(**kwargs_single)
                            ),
                        )

                    single_output_items = self._extract_output_data(single_output)

                    # Store: input → its outputs (handles both 1→1 and 1→N) - use async version
                    # Include FAIR compliance fields
                    if single_output_items:
                        await self.outgate.store_async(
                            input_data=input_item,
                            output_data=single_output_items,
                            component_config=component_config,
                            content_type=self.content_type,
                            run_id=self.run_id,
                        )
                except Exception as e:
                    self.logger.warning(f"Failed to cache item: {e}")
                    pass  # Continue with next item

        end_time = time.time()

        # Log metrics
        self.metrics.log_component_execution(
            component_name=self.component_name,
            component_id=self.component_id,
            start_time=start_time,
            end_time=end_time,
            input_count=len(input_items),
            output_count=len(output_items),
            cache_hits=cache_hits,
            cache_misses=cache_misses,
        )

        # Return component output
        typed_output: Dict[str, Any] = component_output
        return typed_output

    def _extract_component_config(self) -> Dict[str, Any]:
        """
        Extract component configuration for cache keying.

        Returns:
            Dict of component config parameters
        """
        config = {}

        # Try to get component init params
        if hasattr(self.component, "__dict__"):
            for key, value in self.component.__dict__.items():
                # Skip internal/private attributes
                if key.startswith("_"):
                    continue
                # Only include serializable config
                if isinstance(value, (str, int, float, bool, list, dict, type(None))):
                    config[key] = value

        return config

    def _extract_input_data(
        self, kwargs: Dict[str, Any]
    ) -> tuple[Optional[str], List[Any]]:
        """
        Find the main input parameter from kwargs.

        Common Haystack params: documents, queries, text, files, etc.

        Returns:
            (param_name, list_of_items) or (None, [])
        """
        # Common input parameter names in Haystack
        INPUT_PARAMS = ["documents", "queries", "text", "files", "data", "inputs"]

        for param in INPUT_PARAMS:
            if param in kwargs:
                value = kwargs[param]
                # Ensure it's a list
                if not isinstance(value, list):
                    value = [value]
                return param, value

        return None, []

    def _extract_output_data(self, component_output: Dict[str, Any]) -> List[Any]:
        """
        Extract output items from component result.

        Args:
            component_output: Component's return value

        Returns:
            List of output items
        """
        # Common output keys in Haystack
        OUTPUT_KEYS = ["documents", "answers", "results", "embeddings", "chunks"]

        for key in OUTPUT_KEYS:
            if key in component_output:
                value = component_output[key]
                if isinstance(value, list):
                    return value
                return [value]

        # Fallback: return the whole output
        return [component_output]

    def _format_cached_output(self, cached_items: List[tuple]) -> Dict[str, Any]:
        """
        Format cached results for pipeline continuation.

        InGate retrieves actual data from IPFS (not metadata), so we format it
        to match the component's expected output format.

        Args:
            cached_items: List of (input, cached_data) tuples
                         where cached_data is List of actual objects retrieved from IPFS

        Returns:
            Dict with documents key containing cached data
        """
        all_cached_data = []
        for _, cached_data in cached_items:
            if isinstance(cached_data, list):
                all_cached_data.extend(cached_data)
            else:
                all_cached_data.append(cached_data)

        # Return in Haystack format
        # Most components use "documents": converters, chunkers, embedders, retrievers
        # Components that DON'T: generators ("replies"/"answers"), text embedders ("embedding")
        return {"documents": all_cached_data}

    def __getattr__(self, name: str) -> Any:
        """
        Proxy all attributes to the wrapped component.

        This allows Haystack to access __haystack_input__, __haystack_output__, etc.
        """
        return getattr(self.component, name)
