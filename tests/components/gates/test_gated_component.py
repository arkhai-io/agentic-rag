"""Tests for GatedComponent async functionality."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from agentic_rag.components.gates.gated_component import GatedComponent


class MockComponent:
    """Mock component with sync run method."""

    def __init__(self):
        self.run_called = False

    def run(self, **kwargs):
        """Sync run method."""
        self.run_called = True
        documents = kwargs.get("documents", [])
        # Return processed documents
        return {"documents": [f"processed_{doc}" for doc in documents]}


class MockAsyncComponent:
    """Mock component with async run method."""

    def __init__(self):
        self.run_async_called = False
        self.__haystack_supports_async__ = True

    async def run_async(self, **kwargs):
        """Async run method."""
        self.run_async_called = True
        documents = kwargs.get("documents", [])
        # Return processed documents
        return {"documents": [f"async_processed_{doc}" for doc in documents]}


@pytest.fixture
def mock_graph_store():
    """Create a mock GraphStore."""
    store = Mock()
    return store


@pytest.fixture
def mock_storage_client():
    """Create a mock Akave storage client."""
    client = Mock()
    client.upload_any.return_value = {"Hash": "ak_test_hash_123", "Size": "100"}
    client.retrieve_text.return_value = "cached content"
    return client


@pytest.fixture
def mock_ingate():
    """Create a mock InGate."""
    ingate = Mock()
    # Default: all cache misses
    ingate.check_cache_batch.return_value = {
        "cached": [],
        "uncached": ["doc1", "doc2"],
    }
    ingate.check_cache_batch_async = AsyncMock(
        return_value={
            "cached": [],
            "uncached": ["doc1", "doc2"],
        }
    )
    return ingate


@pytest.fixture
def mock_outgate():
    """Create a mock OutGate."""
    outgate = Mock()
    outgate.store_async = AsyncMock()
    return outgate


@pytest.mark.asyncio
@patch("agentic_rag.components.gates.ingate.AkaveClient")
@patch("agentic_rag.components.gates.outgate.AkaveClient")
async def test_run_async_with_async_component(
    mock_outgate_storage,
    mock_ingate_storage,
    mock_graph_store,
    mock_ingate,
    mock_outgate,
    mock_storage_client,
):
    """Test run_async with a component that has run_async method."""
    mock_ingate_storage.return_value = mock_storage_client
    mock_outgate_storage.return_value = mock_storage_client

    component = MockAsyncComponent()
    gated = GatedComponent(
        component=component,
        component_id="test_comp",
        component_name="test_component",
        graph_store=mock_graph_store,
        username="test_user",
    )

    # Mock the gates
    gated.ingate = mock_ingate
    gated.outgate = mock_outgate

    # Run async
    result = await gated.run_async(documents=["doc1", "doc2"])

    # Verify async method was called
    assert component.run_async_called
    assert "documents" in result
    assert len(result["documents"]) == 2
    assert "async_processed_doc1" in result["documents"]
    assert "async_processed_doc2" in result["documents"]


@pytest.mark.asyncio
@patch("agentic_rag.components.gates.ingate.AkaveClient")
@patch("agentic_rag.components.gates.outgate.AkaveClient")
async def test_run_async_with_sync_component(
    mock_outgate_storage,
    mock_ingate_storage,
    mock_graph_store,
    mock_ingate,
    mock_outgate,
    mock_storage_client,
):
    """Test run_async with a component that only has sync run method."""
    mock_ingate_storage.return_value = mock_storage_client
    mock_outgate_storage.return_value = mock_storage_client

    component = MockComponent()
    gated = GatedComponent(
        component=component,
        component_id="test_comp",
        component_name="test_component",
        graph_store=mock_graph_store,
        username="test_user",
    )

    # Mock the gates
    gated.ingate = mock_ingate
    gated.outgate = mock_outgate

    # Run async (should fall back to sync in executor)
    result = await gated.run_async(documents=["doc1", "doc2"])

    # Verify sync method was called
    assert component.run_called
    assert "documents" in result
    assert len(result["documents"]) == 2
    assert "processed_doc1" in result["documents"]
    assert "processed_doc2" in result["documents"]


@pytest.mark.asyncio
@patch("agentic_rag.components.gates.ingate.AkaveClient")
@patch("agentic_rag.components.gates.outgate.AkaveClient")
async def test_run_async_with_cached_items(
    mock_outgate_storage,
    mock_ingate_storage,
    mock_graph_store,
    mock_storage_client,
):
    """Test run_async when all items are in cache."""
    mock_ingate_storage.return_value = mock_storage_client
    mock_outgate_storage.return_value = mock_storage_client

    component = MockAsyncComponent()
    gated = GatedComponent(
        component=component,
        component_id="test_comp",
        component_name="test_component",
        graph_store=mock_graph_store,
        username="test_user",
    )

    # Mock InGate to return all cached (async version)
    mock_ingate = Mock()
    mock_ingate.check_cache_batch_async = AsyncMock(
        return_value={
            "cached": [("doc1", ["cached_result1"]), ("doc2", ["cached_result2"])],
            "uncached": [],
        }
    )
    gated.ingate = mock_ingate
    gated.outgate = Mock()

    # Run async
    result = await gated.run_async(documents=["doc1", "doc2"])

    # Component should not be called since everything is cached
    assert not component.run_async_called
    assert "documents" in result
    assert "cached_result1" in result["documents"]
    assert "cached_result2" in result["documents"]


@pytest.mark.asyncio
@patch("agentic_rag.components.gates.ingate.AkaveClient")
@patch("agentic_rag.components.gates.outgate.AkaveClient")
async def test_run_async_partial_cache(
    mock_outgate_storage,
    mock_ingate_storage,
    mock_graph_store,
    mock_outgate,
    mock_storage_client,
):
    """Test run_async with partial cache hits."""
    mock_ingate_storage.return_value = mock_storage_client
    mock_outgate_storage.return_value = mock_storage_client

    component = MockAsyncComponent()
    gated = GatedComponent(
        component=component,
        component_id="test_comp",
        component_name="test_component",
        graph_store=mock_graph_store,
        username="test_user",
    )

    # Mock InGate to return partial cache (async version)
    mock_ingate = Mock()
    mock_ingate.check_cache_batch_async = AsyncMock(
        return_value={
            "cached": [("doc1", ["cached_result1"])],
            "uncached": ["doc2", "doc3"],
        }
    )
    gated.ingate = mock_ingate
    gated.outgate = mock_outgate

    # Run async
    result = await gated.run_async(documents=["doc1", "doc2", "doc3"])

    # Component should be called for uncached items
    assert component.run_async_called
    assert "documents" in result


@pytest.mark.asyncio
@patch("agentic_rag.components.gates.ingate.AkaveClient")
@patch("agentic_rag.components.gates.outgate.AkaveClient")
async def test_run_async_no_cacheable_inputs(
    mock_outgate_storage,
    mock_ingate_storage,
    mock_graph_store,
    mock_storage_client,
):
    """Test run_async with no cacheable inputs."""
    mock_ingate_storage.return_value = mock_storage_client
    mock_outgate_storage.return_value = mock_storage_client

    component = MockAsyncComponent()
    gated = GatedComponent(
        component=component,
        component_id="test_comp",
        component_name="test_component",
        graph_store=mock_graph_store,
        username="test_user",
    )

    # Run async with non-standard parameter
    await gated.run_async(query="test query")

    # Component should be called directly
    assert component.run_async_called


@pytest.mark.asyncio
@patch("agentic_rag.components.gates.ingate.AkaveClient")
@patch("agentic_rag.components.gates.outgate.AkaveClient")
async def test_run_async_error_handling(
    mock_outgate_storage,
    mock_ingate_storage,
    mock_graph_store,
    mock_ingate,
    mock_storage_client,
):
    """Test run_async handles errors gracefully."""
    mock_ingate_storage.return_value = mock_storage_client
    mock_outgate_storage.return_value = mock_storage_client

    class FailingComponent:
        __haystack_supports_async__ = True

        async def run_async(self, **kwargs):
            raise ValueError("Component failed")

    component = FailingComponent()
    gated = GatedComponent(
        component=component,
        component_id="test_comp",
        component_name="test_component",
        graph_store=mock_graph_store,
        username="test_user",
    )

    gated.ingate = mock_ingate
    gated.outgate = Mock()

    # Should raise the error
    with pytest.raises(ValueError, match="Component failed"):
        await gated.run_async(documents=["doc1"])


@patch("agentic_rag.components.gates.ingate.AkaveClient")
@patch("agentic_rag.components.gates.outgate.AkaveClient")
def test_sync_run_still_works(
    mock_outgate_storage,
    mock_ingate_storage,
    mock_graph_store,
    mock_ingate,
    mock_outgate,
    mock_storage_client,
):
    """Test that sync run method still works after adding async."""
    mock_ingate_storage.return_value = mock_storage_client
    mock_outgate_storage.return_value = mock_storage_client

    component = MockComponent()
    gated = GatedComponent(
        component=component,
        component_id="test_comp",
        component_name="test_component",
        graph_store=mock_graph_store,
        username="test_user",
    )

    # Mock the gates
    gated.ingate = mock_ingate
    gated.outgate = mock_outgate

    # Run sync
    result = gated.run(documents=["doc1", "doc2"])

    # Verify sync method was called
    assert component.run_called
    assert "documents" in result
    assert len(result["documents"]) == 2
