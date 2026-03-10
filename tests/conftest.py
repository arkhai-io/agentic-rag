"""Test configuration and fixtures."""

import os

import pytest

from agentic_rag import Config


@pytest.fixture(scope="session", autouse=True)
def setup_nltk_data():
    """Download required NLTK data for METEOR evaluator tests."""
    try:
        import nltk

        # Download wordnet for METEOR evaluator
        nltk.download("wordnet", quiet=True)
        nltk.download("omw-1.4", quiet=True)  # Open Multilingual Wordnet
    except Exception as e:
        print(f"Warning: Could not download NLTK data: {e}")


@pytest.fixture
def test_config():
    """Create a test Config instance with test credentials."""
    return Config(
        neo4j_uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_username=os.getenv("NEO4J_USERNAME", "neo4j"),
        neo4j_password=os.getenv("NEO4J_PASSWORD", "test_password"),
        neo4j_database=os.getenv("NEO4J_DATABASE"),
        openrouter_api_key=os.getenv("OPENROUTER_API_KEY", "test_openrouter_key"),
        akave_access_key=os.getenv("AKAVE_ACCESS_KEY", "test_akave_access_key"),
        akave_secret_key=os.getenv("AKAVE_SECRET_KEY", "test_akave_secret_key"),
        akave_bucket=os.getenv("AKAVE_BUCKET", "test-bucket"),
        log_level="DEBUG",
    )


@pytest.fixture(autouse=True)
def setup_test_env():
    """Set up test environment variables."""
    # Set test API keys to avoid authentication errors (for backward compatibility)
    os.environ["OPENAI_API_KEY"] = "sk-test_key_for_testing_only_not_real"
    os.environ["HUGGINGFACE_API_TOKEN"] = "test_token_for_testing"

    yield

    # Clean up is not necessary since these are test-only values
