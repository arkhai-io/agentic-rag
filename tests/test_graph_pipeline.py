"""Test graph-based pipeline architecture (Factory + Runner + Neo4j)."""

import os

import pytest

from agentic_rag.components import GraphStore, get_default_registry
from agentic_rag.pipeline import PipelineFactory, PipelineRunner

# Check if Neo4j environment variables are configured
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

NEO4J_AVAILABLE = all([NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD])

# Skip all tests in this module if Neo4j is not configured
pytestmark = pytest.mark.skipif(
    not NEO4J_AVAILABLE,
    reason="Neo4j not configured. Set NEO4J_URI, NEO4J_USERNAME, and NEO4J_PASSWORD environment variables to run these tests.",
)


class TestGraphPipelineArchitecture:
    """Test the new graph-based pipeline system."""

    def setup_method(self):
        """Set up test fixtures."""
        self.graph_store = GraphStore()
        self.factory = PipelineFactory(graph_store=self.graph_store)
        self.runner = PipelineRunner(graph_store=self.graph_store)
        self.registry = get_default_registry()

    def test_factory_builds_pipeline_graph(self):
        """Test that factory builds and stores pipeline graph in Neo4j."""
        pipeline_spec = [
            {"type": "CONVERTER.PDF"},
            {"type": "CHUNKER.DOCUMENT_SPLITTER"},
        ]

        # Build pipeline graph (stores in Neo4j)
        spec = self.factory.build_pipeline_graph(
            pipeline_spec, "test_pipeline", config={}
        )

        assert spec is not None
        assert spec.name == "test_pipeline"
        assert len(spec.components) == 2
        assert spec.components[0].name == "pdf_converter"
        assert spec.components[1].name == "chunker"

    def test_runner_loads_pipeline_graph(self):
        """Test that runner can load pipeline graph from Neo4j."""
        # First, create a pipeline
        pipeline_spec = [
            {"type": "CONVERTER.PDF"},
            {"type": "CHUNKER.DOCUMENT_SPLITTER"},
        ]

        self.factory.build_pipeline_graph(
            pipeline_spec, "load_test_pipeline", config={}
        )

        # Now load it with runner
        self.runner.load_pipeline_graph(
            pipeline_hashes=["load_test_pipeline"], username="test_user"
        )

        # Verify graph data was loaded
        assert self.runner._active_pipeline_graphs
        assert "load_test_pipeline" in self.runner._active_pipeline_graphs

    def test_runner_builds_haystack_components(self):
        """Test that runner builds runtime Haystack components from graph."""
        # Create pipeline
        pipeline_spec = [
            {"type": "CONVERTER.PDF"},
            {"type": "CHUNKER.DOCUMENT_SPLITTER"},
        ]

        self.factory.build_pipeline_graph(
            pipeline_spec, "component_test_pipeline", config={}
        )

        # Load from graph
        self.runner.load_pipeline_graph(
            pipeline_hashes=["component_test_pipeline"], username="test_user"
        )

        # Build Haystack components
        components = self.runner.build_haystack_components_from_graph(
            "component_test_pipeline"
        )

        # Verify components were built
        assert components is not None
        assert len(components) >= 2  # At least the 2 components we added

        # Verify active pipeline is set
        assert "component_test_pipeline" in self.runner._active_pipelines
        assert len(self.runner._active_pipelines["component_test_pipeline"]) >= 2

    def test_runner_requires_graph_store(self):
        """Test that runner requires GraphStore to be configured."""
        runner_no_store = PipelineRunner()

        with pytest.raises(RuntimeError, match="No graph store configured"):
            runner_no_store.load_pipeline_graph(
                pipeline_hashes=["test"], username="test"
            )

    def test_pipeline_with_config(self):
        """Test pipeline creation with custom configuration."""
        pipeline_spec = [
            {"type": "CHUNKER.DOCUMENT_SPLITTER"},
        ]

        config = {
            "chunker": {
                "chunk_size": 500,
                "chunk_overlap": 50,
            }
        }

        spec = self.factory.build_pipeline_graph(
            pipeline_spec, "config_test_pipeline", config=config
        )

        assert spec.components[0].get_config()["chunk_size"] == 500
        assert spec.components[0].get_config()["chunk_overlap"] == 50

    def test_component_registry_integration(self):
        """Test that components are properly looked up from registry."""
        spec = self.registry.get_component_spec("pdf_converter")

        assert spec is not None
        assert spec.name == "pdf_converter"
        assert spec.component_type.value == "converter"

    def test_invalid_pipeline_hash_handling(self):
        """Test handling of invalid pipeline hashes."""
        self.runner.load_pipeline_graph(
            pipeline_hashes=["nonexistent_pipeline"], username="test_user"
        )

        # Should load empty/missing data for non-existent pipeline
        # Either not in dict or empty list
        if "nonexistent_pipeline" in self.runner._active_pipeline_graphs:
            assert len(self.runner._active_pipeline_graphs["nonexistent_pipeline"]) == 0
        # If not in dict, that's also fine - pipeline doesn't exist


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
