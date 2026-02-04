"""Configuration management for agentic-rag SDK.

This module provides a centralized configuration system that supports both:
1. Programmatic configuration (for SDK usage)
2. Environment variable fallback (for backward compatibility)

Usage as SDK:
    ```python
    from agentic_rag import Config, PipelineFactory

    # Initialize with explicit configuration
    config = Config(
        neo4j_uri="bolt://localhost:7687",
        neo4j_username="neo4j",
        neo4j_password="password",
        openrouter_api_key="your-key",
        akave_access_key="your-access-key",
        akave_secret_key="your-secret-key",
        akave_bucket="your-bucket"
    )

    # Use with PipelineFactory
    factory = PipelineFactory(config=config, username="alice")
    ```

Environment Variables (fallback):
    - NEO4J_URI
    - NEO4J_USERNAME
    - NEO4J_PASSWORD
    - NEO4J_DATABASE
    - OPENROUTER_API_KEY
    - AKAVE_ACCESS_KEY
    - AKAVE_SECRET_KEY
    - AKAVE_BUCKET
    - AKAVE_ENDPOINT
    - AGENTIC_RAG_LOG_LEVEL
    - AGENTIC_RAG_LOG_FILE
"""

import os
from typing import Optional


class Config:
    """
    Central configuration for agentic-rag SDK.

    Attributes:
        neo4j_uri: Neo4j database URI
        neo4j_username: Neo4j username
        neo4j_password: Neo4j password
        neo4j_database: Neo4j database name
        openrouter_api_key: OpenRouter API key for LLM access
        akave_access_key: Akave S3 access key
        akave_secret_key: Akave S3 secret key
        akave_bucket: Akave S3 bucket name
        akave_endpoint: Akave S3 endpoint URL
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
        agentic_root_dir: Root directory for agentic-rag data storage
    """

    def __init__(
        self,
        neo4j_uri: Optional[str] = None,
        neo4j_username: Optional[str] = None,
        neo4j_password: Optional[str] = None,
        neo4j_database: Optional[str] = None,
        openrouter_api_key: Optional[str] = None,
        akave_access_key: Optional[str] = None,
        akave_secret_key: Optional[str] = None,
        akave_bucket: Optional[str] = None,
        akave_endpoint: Optional[str] = None,
        log_level: Optional[str] = None,
        log_file: Optional[str] = None,
        agentic_root_dir: Optional[str] = None,
        component_cache_size: Optional[int] = None,
    ):
        """
        Initialize configuration.

        All parameters are optional and will fall back to environment variables
        if not provided.

        Args:
            neo4j_uri: Neo4j connection URI (defaults to NEO4J_URI env var)
            neo4j_username: Neo4j username (defaults to NEO4J_USERNAME env var)
            neo4j_password: Neo4j password (defaults to NEO4J_PASSWORD env var)
            neo4j_database: Neo4j database name (defaults to NEO4J_DATABASE env var)
            openrouter_api_key: OpenRouter API key (defaults to OPENROUTER_API_KEY env var)
            akave_access_key: Akave S3 access key (defaults to AKAVE_ACCESS_KEY env var)
            akave_secret_key: Akave S3 secret key (defaults to AKAVE_SECRET_KEY env var)
            akave_bucket: Akave bucket name (defaults to AKAVE_BUCKET env var or "agentic-rag")
            akave_endpoint: Akave endpoint URL (defaults to AKAVE_ENDPOINT env var)
            log_level: Log level (defaults to AGENTIC_RAG_LOG_LEVEL env var or INFO)
            log_file: Log file path (defaults to AGENTIC_RAG_LOG_FILE env var)
            agentic_root_dir: Root directory for agentic-rag data (defaults to AGENTIC_ROOT_DIR env var or ./data)
            component_cache_size: Max components to cache (defaults to AGENTIC_COMPONENT_CACHE_SIZE env var or 5)
        """
        # Neo4j configuration
        self.neo4j_uri = neo4j_uri or os.getenv("NEO4J_URI")
        self.neo4j_username = neo4j_username or os.getenv("NEO4J_USERNAME")
        self.neo4j_password = neo4j_password or os.getenv("NEO4J_PASSWORD")
        self.neo4j_database = neo4j_database or os.getenv("NEO4J_DATABASE")

        # API keys
        self.openrouter_api_key = openrouter_api_key or os.getenv("OPENROUTER_API_KEY")

        # Akave S3-compatible storage configuration
        self.akave_access_key = akave_access_key or os.getenv("AKAVE_ACCESS_KEY")
        self.akave_secret_key = akave_secret_key or os.getenv("AKAVE_SECRET_KEY")
        self.akave_bucket = akave_bucket or os.getenv("AKAVE_BUCKET", "agentic-rag")
        self.akave_endpoint = akave_endpoint or os.getenv(
            "AKAVE_ENDPOINT", "https://o3-rc3.akave.xyz"
        )

        # Logging configuration
        self.log_level = log_level or os.getenv("AGENTIC_RAG_LOG_LEVEL") or "INFO"
        self.log_file = log_file or os.getenv("AGENTIC_RAG_LOG_FILE")

        # Storage configuration
        self.agentic_root_dir = (
            agentic_root_dir or os.getenv("AGENTIC_ROOT_DIR") or "./data"
        )

        # Cache configuration
        self.component_cache_size = component_cache_size or int(
            os.getenv("AGENTIC_COMPONENT_CACHE_SIZE", "5")
        )

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a configuration value by key.

        Args:
            key: Configuration key (e.g., 'neo4j_uri', 'openrouter_api_key')
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        return getattr(self, key, default)

    def validate_neo4j(self) -> bool:
        """
        Check if Neo4j configuration is complete.

        Returns:
            True if all required Neo4j settings are present
        """
        return all(
            [
                self.neo4j_uri,
                self.neo4j_username,
                self.neo4j_password,
            ]
        )

    def validate_openrouter(self) -> bool:
        """
        Check if OpenRouter API key is configured.

        Returns:
            True if OpenRouter API key is present
        """
        return self.openrouter_api_key is not None

    def validate_akave(self) -> bool:
        """
        Check if Akave S3 credentials are configured.

        Returns:
            True if Akave access key and secret key are present
        """
        return self.akave_access_key is not None and self.akave_secret_key is not None

    def get_project_path(self, username: str, project: str) -> str:
        """
        Get storage path for a specific user project.

        Args:
            username: Username
            project: Project name

        Returns:
            Path in format: {agentic_root_dir}/{username}/{project}/
        """
        import os

        return os.path.join(self.agentic_root_dir, username, project)

    def to_dict(self) -> dict:
        """
        Export configuration as dictionary.

        Returns:
            Dictionary with all configuration values (passwords masked)
        """
        return {
            "neo4j_uri": self.neo4j_uri,
            "neo4j_username": self.neo4j_username,
            "neo4j_password": "***" if self.neo4j_password else None,
            "neo4j_database": self.neo4j_database,
            "openrouter_api_key": "***" if self.openrouter_api_key else None,
            "akave_access_key": "***" if self.akave_access_key else None,
            "akave_secret_key": "***" if self.akave_secret_key else None,
            "akave_bucket": self.akave_bucket,
            "akave_endpoint": self.akave_endpoint,
            "log_level": self.log_level,
            "log_file": self.log_file,
            "agentic_root_dir": self.agentic_root_dir,
            "component_cache_size": self.component_cache_size,
        }

    def __repr__(self) -> str:
        """String representation with masked secrets."""
        return f"Config({self.to_dict()})"


# Global configuration instance (can be set once for the entire application)
_global_config: Optional[Config] = None


def set_global_config(config: Config) -> None:
    """
    Set the global configuration instance.

    This allows setting configuration once and having it used throughout
    the application without passing it to every component.

    Args:
        config: Configuration instance to use globally

    Example:
        ```python
        from agentic_rag import Config, set_global_config

        config = Config(
            neo4j_uri="bolt://localhost:7687",
            neo4j_username="neo4j",
            neo4j_password="password"
        )
        set_global_config(config)

        # Now all components will use this config by default
        ```
    """
    global _global_config
    _global_config = config


def get_global_config() -> Optional[Config]:
    """
    Get the global configuration instance.

    Returns:
        Global Config instance or None if not set
    """
    return _global_config


def get_config(config: Optional[Config] = None) -> Config:
    """
    Get configuration instance with fallback logic.

    Priority:
    1. Explicitly passed config
    2. Global config (if set via set_global_config)
    3. New Config() with environment variable fallback

    Args:
        config: Optional explicit configuration

    Returns:
        Configuration instance to use
    """
    if config is not None:
        return config

    if _global_config is not None:
        return _global_config

    # Fallback: create new config from environment variables
    return Config()
