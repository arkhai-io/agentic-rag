"""Test configuration and fixtures."""

import os

import pytest


def pytest_configure(config):
    """Configure pytest with custom markers and warnings."""
    # Check if Neo4j is configured and print warning if not
    neo4j_uri = os.getenv("NEO4J_URI")
    neo4j_username = os.getenv("NEO4J_USERNAME")
    neo4j_password = os.getenv("NEO4J_PASSWORD")

    if not all([neo4j_uri, neo4j_username, neo4j_password]):
        print("\n" + "=" * 70)
        print("WARNING: Neo4j environment variables not set.")
        print("Tests requiring Neo4j will be skipped.")
        print()
        print("To run Neo4j tests, set the following environment variables:")
        print("  - NEO4J_URI")
        print("  - NEO4J_USERNAME")
        print("  - NEO4J_PASSWORD")
        print("=" * 70 + "\n")


@pytest.fixture(autouse=True)
def setup_test_env():
    """Set up test environment variables."""
    # Set test API keys to avoid authentication errors
    os.environ["OPENAI_API_KEY"] = "sk-test_key_for_testing_only_not_real"
    os.environ["HUGGINGFACE_API_TOKEN"] = "test_token_for_testing"

    yield

    # Clean up is not necessary since these are test-only values
