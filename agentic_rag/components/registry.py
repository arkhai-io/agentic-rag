"""Component registry for mapping component names to Haystack classes."""

import hashlib
import json
from collections import OrderedDict
from typing import Any, Dict, List, Optional

from ..types import (
    ComponentSpec,
    ComponentType,
    DataType,
    PipelineUsage,
    create_haystack_component,
)
from ..types.node_types import ContentType


class ComponentRegistry:
    """
    Registry for managing component specifications and caching instances.

    Features:
    - Maps component names to specifications
    - Caches instantiated components (LRU) to prevent reloading heavy models
    - Ensures efficient resource usage across pipelines
    """

    def __init__(self, max_cache_size: int = 10) -> None:
        self._components: Dict[str, ComponentSpec] = {}
        # LRU Cache: key -> component_instance
        self._instance_cache: OrderedDict[str, Any] = OrderedDict()
        self._max_cache_size = max_cache_size
        self._initialize_default_components()

    def register_component(self, spec: ComponentSpec) -> None:
        """Register a component specification."""
        self._components[spec.name] = spec

    def get_component_spec(self, name: str) -> Optional[ComponentSpec]:
        """Get component specification by name."""
        return self._components.get(name)

    def list_components(self) -> List[str]:
        """List all registered component names."""
        return list(self._components.keys())

    def get_components_by_type(
        self, component_type: ComponentType
    ) -> List[ComponentSpec]:
        """Get all components of a specific type."""
        return [
            spec
            for spec in self._components.values()
            if spec.component_type == component_type
        ]

    def get_component_instance(self, spec: ComponentSpec) -> Any:
        """
        Get or create a component instance with LRU caching.

        Args:
            spec: Configured component specification

        Returns:
            Instantiated Haystack component
        """
        # Skip caching for components that shouldn't be shared or are lightweight
        # e.g., DocumentStores and Writers which might have unique connection states
        skip_caching = (
            spec.component_type == ComponentType.WRITER or "store" in spec.name.lower()
        )

        if skip_caching:
            return create_haystack_component(spec)

        # Generate cache key based on component class and sorted configuration
        # This ensures identical configs share the same instance
        config_str = json.dumps(spec.get_config(), sort_keys=True)
        config_hash = hashlib.md5(config_str.encode()).hexdigest()
        cache_key = f"{spec.haystack_class}_{config_hash}"

        # 1. Cache Hit
        if cache_key in self._instance_cache:
            # Move to end (mark as recently used)
            self._instance_cache.move_to_end(cache_key)
            return self._instance_cache[cache_key]

        # 2. Cache Miss - Create new instance
        component = create_haystack_component(spec)

        # 3. Eviction (if full)
        if len(self._instance_cache) >= self._max_cache_size:
            # Pop the first item (Least Recently Used)
            oldest_key, _ = self._instance_cache.popitem(last=False)
            # Python GC will handle the cleanup

        # 4. Store new component
        self._instance_cache[cache_key] = component
        return component

    def clear_cache(self) -> None:
        """Clear the component instance cache."""
        self._instance_cache.clear()

    def _initialize_default_components(self) -> None:
        """Initialize default Haystack component mappings."""

        # Converters - produce DOCUMENT
        self.register_component(
            ComponentSpec(
                name="pdf_converter",
                component_type=ComponentType.CONVERTER,
                haystack_class="haystack.components.converters.PyPDFToDocument",
                input_types=[DataType.LIST_BYTE_STREAM],
                output_types=[DataType.LIST_DOCUMENT],
                pipeline_usage=PipelineUsage.INDEXING,
                default_config={},
                produces_content_type=ContentType.DOCUMENT.value,
            )
        )

        self.register_component(
            ComponentSpec(
                name="docx_converter",
                component_type=ComponentType.CONVERTER,
                haystack_class="haystack.components.converters.DOCXToDocument",
                input_types=[DataType.LIST_BYTE_STREAM],
                output_types=[DataType.LIST_DOCUMENT],
                pipeline_usage=PipelineUsage.INDEXING,
                default_config={},
                produces_content_type=ContentType.DOCUMENT.value,
            )
        )

        self.register_component(
            ComponentSpec(
                name="markdown_converter",
                component_type=ComponentType.CONVERTER,
                haystack_class="haystack.components.converters.MarkdownToDocument",
                input_types=[DataType.LIST_BYTE_STREAM],
                output_types=[DataType.LIST_DOCUMENT],
                pipeline_usage=PipelineUsage.INDEXING,
                default_config={},
                produces_content_type=ContentType.DOCUMENT.value,
            )
        )

        self.register_component(
            ComponentSpec(
                name="html_converter",
                component_type=ComponentType.CONVERTER,
                haystack_class="haystack.components.converters.HTMLToDocument",
                input_types=[DataType.LIST_BYTE_STREAM],
                output_types=[DataType.LIST_DOCUMENT],
                pipeline_usage=PipelineUsage.INDEXING,
                default_config={},
                produces_content_type=ContentType.DOCUMENT.value,
            )
        )

        self.register_component(
            ComponentSpec(
                name="text_converter",
                component_type=ComponentType.CONVERTER,
                haystack_class="haystack.components.converters.TextFileToDocument",
                input_types=[DataType.LIST_BYTE_STREAM],
                output_types=[DataType.LIST_DOCUMENT],
                pipeline_usage=PipelineUsage.INDEXING,
                default_config={},
                produces_content_type=ContentType.DOCUMENT.value,
            )
        )

        self.register_component(
            ComponentSpec(
                name="marker_pdf_converter",
                component_type=ComponentType.CONVERTER,
                haystack_class="agentic_rag.components.converters.MarkerPDFToDocument",
                input_types=[DataType.LIST_BYTE_STREAM],
                output_types=[DataType.LIST_DOCUMENT],
                pipeline_usage=PipelineUsage.INDEXING,
                default_config={
                    "languages": "en",
                    "output_format": "markdown",
                    "store_full_path": False,
                },
                produces_content_type=ContentType.DOCUMENT.value,
            )
        )

        self.register_component(
            ComponentSpec(
                name="markitdown_pdf_converter",
                component_type=ComponentType.CONVERTER,
                haystack_class="agentic_rag.components.converters.markitdown_pdf_converter.MarkItDownPDFToDocument",
                input_types=[DataType.LIST_BYTE_STREAM],
                output_types=[DataType.LIST_DOCUMENT],
                pipeline_usage=PipelineUsage.INDEXING,
                default_config={
                    "store_full_path": False,
                },
                produces_content_type=ContentType.DOCUMENT.value,
            )
        )

        # Chunkers/Splitters - produce CHUNK
        self.register_component(
            ComponentSpec(
                name="chunker",
                component_type=ComponentType.CHUNKER,
                haystack_class="haystack.components.preprocessors.DocumentSplitter",
                input_types=[DataType.LIST_DOCUMENT],
                output_types=[DataType.LIST_DOCUMENT],
                pipeline_usage=PipelineUsage.INDEXING,
                default_config={"split_by": "sentence", "split_length": 512},
                produces_content_type=ContentType.CHUNK.value,
            )
        )

        # Custom Chunkers - produce CHUNK
        self.register_component(
            ComponentSpec(
                name="markdown_aware_chunker",
                component_type=ComponentType.CHUNKER,
                haystack_class="agentic_rag.components.chunkers.MarkdownAwareChunker",
                input_types=[DataType.LIST_DOCUMENT],
                output_types=[DataType.LIST_DOCUMENT],
                pipeline_usage=PipelineUsage.INDEXING,
                default_config={"chunk_size": 1000, "chunk_overlap": 100},
                produces_content_type=ContentType.CHUNK.value,
            )
        )

        self.register_component(
            ComponentSpec(
                name="semantic_chunker",
                component_type=ComponentType.CHUNKER,
                haystack_class="agentic_rag.components.chunkers.SemanticChunker",
                input_types=[DataType.LIST_DOCUMENT],
                output_types=[DataType.LIST_DOCUMENT],
                pipeline_usage=PipelineUsage.INDEXING,
                default_config={
                    "min_chunk_size": 200,
                    "max_chunk_size": 1000,
                    "overlap_size": 50,
                },
                produces_content_type=ContentType.CHUNK.value,
            )
        )

        # Embedders - produce EMBEDDING
        self.register_component(
            ComponentSpec(
                name="embedder",
                component_type=ComponentType.EMBEDDER,
                haystack_class="haystack.components.embedders.SentenceTransformersTextEmbedder",
                input_types=[DataType.LIST_STRING],
                output_types=[DataType.LIST_LIST_FLOAT],
                pipeline_usage=PipelineUsage.RETRIEVAL,
                default_config={"model": "sentence-transformers/all-MiniLM-L6-v2"},
                produces_content_type=ContentType.EMBEDDING.value,
            )
        )

        self.register_component(
            ComponentSpec(
                name="document_embedder",
                component_type=ComponentType.EMBEDDER,
                haystack_class="haystack.components.embedders.SentenceTransformersDocumentEmbedder",
                input_types=[DataType.LIST_DOCUMENT],
                output_types=[DataType.LIST_DOCUMENT],  # Documents with embeddings
                pipeline_usage=PipelineUsage.INDEXING,
                default_config={"model": "sentence-transformers/all-MiniLM-L6-v2"},
                produces_content_type=ContentType.EMBEDDING.value,
            )
        )

        # Document Stores are handled separately as dependencies, not pipeline components

        # Retrievers

        # Chroma Retrievers
        self.register_component(
            ComponentSpec(
                name="chroma_embedding_retriever",
                component_type=ComponentType.RETRIEVER,
                haystack_class="haystack_integrations.components.retrievers.chroma.ChromaEmbeddingRetriever",
                input_types=[DataType.LIST_FLOAT],
                output_types=[DataType.LIST_DOCUMENT],
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],  # Document store will be passed during component creation
                default_config={"top_k": 10},
            )
        )

        # Qdrant Retrievers
        self.register_component(
            ComponentSpec(
                name="qdrant_embedding_retriever",
                component_type=ComponentType.RETRIEVER,
                haystack_class="haystack_integrations.components.retrievers.qdrant.QdrantEmbeddingRetriever",
                input_types=[DataType.LIST_FLOAT],
                output_types=[DataType.LIST_DOCUMENT],
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],  # Document store will be passed during component creation
                default_config={"top_k": 10},
            )
        )

        # Rankers
        self.register_component(
            ComponentSpec(
                name="sentence_transformers_similarity_ranker",
                component_type=ComponentType.RERANKER,
                haystack_class="haystack.components.rankers.SentenceTransformersSimilarityRanker",
                input_types=[DataType.LIST_DOCUMENT, DataType.STRING],
                output_types=[DataType.LIST_DOCUMENT],
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={
                    "model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
                    "top_k": 10,
                    "device": None,  # Can be "cpu" or "cuda"
                    "backend": "torch",  # Can be "torch", "onnx", or "openvino"
                },
                parallelizable=False,
            )
        )

        # Prompt Builders
        self.register_component(
            ComponentSpec(
                name="prompt_builder",
                component_type=ComponentType.GENERATOR,  # Using GENERATOR type since it's part of generation pipeline
                haystack_class="haystack.components.builders.PromptBuilder",
                input_types=[DataType.LIST_DOCUMENT, DataType.STRING],
                output_types=[DataType.STRING],
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={
                    "template": (
                        "Answer the question based only on the provided documents.\n\n"
                        "Question: {{query}}\n\n"
                        "Documents:\n"
                        "{% for doc in documents %}\n"
                        "{{ doc.content }}\n"
                        "---\n"
                        "{% endfor %}\n\n"
                        "Answer:"
                    )
                },
                parallelizable=False,
            )
        )

        # Generators - Only verified existing ones
        self.register_component(
            ComponentSpec(
                name="generator",
                component_type=ComponentType.GENERATOR,
                haystack_class="haystack.components.generators.OpenAIGenerator",
                input_types=[DataType.LIST_DOCUMENT, DataType.STRING],
                output_types=[DataType.STRING],
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],  # Retriever will be connected via pipeline
                default_config={"model": "gpt-3.5-turbo"},
                parallelizable=False,
            )
        )

        self.register_component(
            ComponentSpec(
                name="openrouter_generator",
                component_type=ComponentType.GENERATOR,
                haystack_class="agentic_rag.components.generators.OpenRouterGenerator",
                input_types=[DataType.LIST_DOCUMENT, DataType.STRING],
                output_types=[DataType.STRING],
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={
                    "model": "openai/gpt-3.5-turbo",
                    "generation_kwargs": {
                        "temperature": 0.7,
                        "max_tokens": 1000,
                    },
                },
                parallelizable=False,
            )
        )

        # Writers - Document indexing components
        self.register_component(
            ComponentSpec(
                name="chroma_document_writer",
                component_type=ComponentType.WRITER,
                haystack_class="haystack.components.writers.DocumentWriter",
                input_types=[DataType.LIST_DOCUMENT],
                output_types=[
                    DataType.DICT
                ],  # Returns metadata about written documents
                pipeline_usage=PipelineUsage.INDEXING,
                dependencies=[],
                default_config={"root_dir": "."},
                parallelizable=True,
            )
        )

        self.register_component(
            ComponentSpec(
                name="qdrant_document_writer",
                component_type=ComponentType.WRITER,
                haystack_class="haystack.components.writers.DocumentWriter",
                input_types=[DataType.LIST_DOCUMENT],
                output_types=[
                    DataType.DICT
                ],  # Returns metadata about written documents
                pipeline_usage=PipelineUsage.INDEXING,
                dependencies=[],
                default_config={"root_dir": ".", "embedding_dim": 768},
                parallelizable=True,
            )
        )

        # Evaluators - Evaluation components (used in retrieval pipelines)
        # Note: Focus on answer quality only, no document evaluation for now
        # All evaluators package everything into a single eval_data dict
        self.register_component(
            ComponentSpec(
                name="reference_free_evaluator",
                component_type=ComponentType.EVALUATOR,
                haystack_class="agentic_rag.components.evaluators.ReferenceFreeEvaluator",
                input_types=[DataType.STRING, DataType.LIST_STRING],  # query, replies
                output_types=[DataType.DICT],  # Single eval_data dict with everything
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={},
                parallelizable=False,
            )
        )

        # Grounded Evaluation Metrics - Lexical Overlap
        self.register_component(
            ComponentSpec(
                name="bleu_evaluator",
                component_type=ComponentType.EVALUATOR,
                haystack_class="agentic_rag.components.evaluators.BLEUEvaluator",
                input_types=[DataType.STRING, DataType.LIST_STRING],  # query, replies
                output_types=[DataType.DICT],  # Single eval_data dict
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={"max_n": 4, "smoothing": True},
                parallelizable=False,
            )
        )

        self.register_component(
            ComponentSpec(
                name="rouge_evaluator",
                component_type=ComponentType.EVALUATOR,
                haystack_class="agentic_rag.components.evaluators.ROUGEEvaluator",
                input_types=[DataType.STRING, DataType.LIST_STRING],  # query, replies
                output_types=[DataType.DICT],  # Single eval_data dict
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={"rouge_type": "rougeL", "use_stemmer": True},
                parallelizable=False,
            )
        )

        self.register_component(
            ComponentSpec(
                name="meteor_evaluator",
                component_type=ComponentType.EVALUATOR,
                haystack_class="agentic_rag.components.evaluators.METEOREvaluator",
                input_types=[DataType.STRING, DataType.LIST_STRING],  # query, replies
                output_types=[DataType.DICT],  # Single eval_data dict
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={"alpha": 0.9, "beta": 3.0, "gamma": 0.5},
                parallelizable=False,
            )
        )

        # Grounded Evaluation Metrics - LLM-as-Judge
        self.register_component(
            ComponentSpec(
                name="answer_quality_evaluator",
                component_type=ComponentType.EVALUATOR,
                haystack_class="agentic_rag.components.evaluators.AnswerQualityEvaluator",
                input_types=[DataType.STRING, DataType.LIST_STRING],  # query, replies
                output_types=[DataType.DICT],  # Single eval_data dict
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={
                    "model": "anthropic/claude-3.5-sonnet",
                    "base_url": "https://openrouter.ai/api/v1",
                },
                parallelizable=False,
            )
        )

        self.register_component(
            ComponentSpec(
                name="fact_matching_evaluator",
                component_type=ComponentType.EVALUATOR,
                haystack_class="agentic_rag.components.evaluators.FactMatchingEvaluator",
                input_types=[DataType.STRING, DataType.LIST_STRING],  # query, replies
                output_types=[DataType.DICT],  # Single eval_data dict
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={
                    "llm_model": "anthropic/claude-3.5-sonnet",
                    "embedding_model": "all-MiniLM-L6-v2",
                    "base_url": "https://openrouter.ai/api/v1",
                    "similarity_threshold": 0.75,
                    "matching_strategy": "greedy",
                },
                parallelizable=False,
            )
        )

        self.register_component(
            ComponentSpec(
                name="longqa_answer_evaluator",
                component_type=ComponentType.EVALUATOR,
                haystack_class="agentic_rag.components.evaluators.LongQAAnswerEvaluator",
                input_types=[DataType.STRING, DataType.LIST_STRING],  # query, replies
                output_types=[DataType.DICT],  # Single eval_data dict
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={
                    "model": "anthropic/claude-3.5-sonnet",
                    "base_url": "https://openrouter.ai/api/v1",
                },
                parallelizable=False,
            )
        )

        self.register_component(
            ComponentSpec(
                name="morqa_faithfulness_evaluator",
                component_type=ComponentType.EVALUATOR,
                haystack_class="agentic_rag.components.evaluators.MORQAFaithfulnessEvaluator",
                input_types=[DataType.STRING, DataType.LIST_STRING],  # query, replies
                output_types=[DataType.DICT],  # Single eval_data dict
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={
                    "model": "anthropic/claude-3.5-sonnet",
                    "base_url": "https://openrouter.ai/api/v1",
                },
                parallelizable=False,
            )
        )

        # Ungrounded Evaluation Metrics - Answer Quality (No Gold Standard)
        self.register_component(
            ComponentSpec(
                name="answer_structure_evaluator",
                component_type=ComponentType.EVALUATOR,
                haystack_class="agentic_rag.components.evaluators.AnswerStructureEvaluator",
                input_types=[DataType.STRING, DataType.LIST_STRING],  # query, replies
                output_types=[DataType.DICT],  # Single eval_data dict
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={
                    "model": "anthropic/claude-3.5-sonnet",
                    "base_url": "https://openrouter.ai/api/v1",
                },
                parallelizable=False,
            )
        )

        self.register_component(
            ComponentSpec(
                name="coherence_evaluator",
                component_type=ComponentType.EVALUATOR,
                haystack_class="agentic_rag.components.evaluators.CoherenceEvaluator",
                input_types=[DataType.STRING, DataType.LIST_STRING],  # query, replies
                output_types=[DataType.DICT],  # Single eval_data dict
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={"embedding_model": "all-MiniLM-L6-v2"},
                parallelizable=False,
            )
        )

        self.register_component(
            ComponentSpec(
                name="communication_quality_evaluator",
                component_type=ComponentType.EVALUATOR,
                haystack_class="agentic_rag.components.evaluators.CommunicationQualityEvaluator",
                input_types=[DataType.STRING, DataType.LIST_STRING],  # query, replies
                output_types=[DataType.DICT],  # Single eval_data dict
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={
                    "model": "anthropic/claude-3.5-sonnet",
                    "base_url": "https://openrouter.ai/api/v1",
                },
                parallelizable=False,
            )
        )

        self.register_component(
            ComponentSpec(
                name="readability_evaluator",
                component_type=ComponentType.EVALUATOR,
                haystack_class="agentic_rag.components.evaluators.ReadabilityEvaluator",
                input_types=[DataType.STRING, DataType.LIST_STRING],  # query, replies
                output_types=[DataType.DICT],  # Single eval_data dict
                pipeline_usage=PipelineUsage.RETRIEVAL,
                dependencies=[],
                default_config={},
                parallelizable=False,
            )
        )


# Global registry instance
_default_registry: Optional[ComponentRegistry] = None


def get_default_registry() -> ComponentRegistry:
    """Get the default component registry."""
    global _default_registry
    if _default_registry is None:
        from ..config import get_global_config

        config = get_global_config()
        cache_size = config.component_cache_size if config else 5
        _default_registry = ComponentRegistry(max_cache_size=cache_size)
    return _default_registry
