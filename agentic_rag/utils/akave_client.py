"""Akave S3-compatible storage client for uploading and retrieving data.

Akave provides decentralized S3-compatible storage with encryption and
erasure coding. This client uses boto3 to interact with the Akave O3 API.

Endpoint: https://o3-rc3.akave.xyz
"""

import hashlib
import io
import json
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import aioboto3
import boto3
from botocore.config import Config as BotoConfig
from dotenv import load_dotenv

if TYPE_CHECKING:
    from ..config import Config

load_dotenv()

# Default Akave O3 endpoint
AKAVE_ENDPOINT = "https://o3-rc3.akave.xyz"
AKAVE_REGION = "us-east-1"  # S3 requires a region, use default


class AkaveClient:
    """
    Client for interacting with Akave S3-compatible storage.

    Supports:
    - Upload files/text/JSON as S3 objects
    - Upload raw bytes/buffers
    - Retrieve data by object key
    - Content-addressed storage (hash-based keys)

    The client maintains compatibility with the LighthouseClient interface
    by returning similar response formats.
    """

    def __init__(
        self,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket: Optional[str] = None,
        endpoint: Optional[str] = None,
        config: Optional["Config"] = None,
        timeout: float = 300.0,
    ):
        """
        Initialize Akave client.

        Args:
            access_key: Akave S3 access key (overrides config)
            secret_key: Akave S3 secret key (overrides config)
            bucket: S3 bucket name for storage (overrides config)
            endpoint: Akave endpoint URL (defaults to o3-rc3.akave.xyz)
            config: Config object with Akave credentials
            timeout: Timeout for API requests in seconds (default: 300.0)
        """
        # Priority: explicit params > config object > env vars
        if config is not None:
            self.access_key = access_key or config.akave_access_key
            self.secret_key = secret_key or config.akave_secret_key
            self.bucket = bucket or config.akave_bucket
            self.endpoint = endpoint or config.akave_endpoint or AKAVE_ENDPOINT
        else:
            import os

            self.access_key = access_key or os.getenv("AKAVE_ACCESS_KEY") or ""
            self.secret_key = secret_key or os.getenv("AKAVE_SECRET_KEY") or ""
            self.bucket = bucket or os.getenv("AKAVE_BUCKET", "agentic-rag")
            self.endpoint = endpoint or os.getenv("AKAVE_ENDPOINT") or AKAVE_ENDPOINT

        if not self.access_key or not self.secret_key:
            raise ValueError(
                "Akave credentials required. Provide via config parameter:\n"
                "  config = Config(akave_access_key='...', akave_secret_key='...')\n"
                "  AkaveClient(config=config)\n"
                "Or set AKAVE_ACCESS_KEY and AKAVE_SECRET_KEY environment variables."
            )

        self.timeout = timeout

        # Configure boto3 client
        boto_config = BotoConfig(
            connect_timeout=30,
            read_timeout=int(timeout),
            retries={"max_attempts": 3},
        )

        # Create sync S3 client
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=AKAVE_REGION,
            config=boto_config,
        )

        # Store session for async client creation
        self._boto_config = boto_config

        # Ensure bucket exists
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        """Create bucket if it doesn't exist."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
        except Exception:
            try:
                self.s3_client.create_bucket(Bucket=self.bucket)
            except Exception:
                # Bucket might already exist or we don't have permissions
                pass

    def _generate_key(self, data: bytes, prefix: str = "") -> str:
        """
        Generate content-addressed key from data hash.

        Args:
            data: Raw bytes to hash
            prefix: Optional prefix for organization

        Returns:
            Key in format: {prefix}/{sha256_hash[:16]}
        """
        hash_obj = hashlib.sha256(data)
        key = f"ak_{hash_obj.hexdigest()[:16]}"
        if prefix:
            key = f"{prefix}/{key}"
        return key

    def upload_text(self, text: str, name: Optional[str] = None) -> Dict[str, Any]:
        """
        Upload text to Akave.

        Args:
            text: Text content to upload
            name: Optional name (used as prefix)

        Returns:
            {
                "Name": str,
                "Hash": str (object key),
                "Size": str
            }
        """
        text_bytes = text.encode("utf-8")
        return self.upload_buffer(text_bytes, name=name)

    async def upload_text_async(
        self, text: str, name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Async version of upload_text."""
        text_bytes = text.encode("utf-8")
        return await self.upload_buffer_async(text_bytes, name=name)

    def upload_buffer(
        self, data: Union[bytes, io.BytesIO], name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Upload raw bytes/buffer to Akave.

        Args:
            data: Bytes or BytesIO buffer
            name: Optional name/prefix

        Returns:
            {
                "Name": str,
                "Hash": str (object key),
                "Size": str
            }
        """
        # Convert BytesIO to bytes if needed
        if isinstance(data, io.BytesIO):
            data = data.getvalue()

        # Generate content-addressed key
        prefix = name.replace(".", "_") if name else "data"
        key = self._generate_key(data, prefix)

        # Upload to S3
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
        )

        return {
            "Name": name or key,
            "Hash": key,  # Maintain compatibility with IPFS client interface
            "Size": str(len(data)),
        }

    async def upload_buffer_async(
        self, data: Union[bytes, io.BytesIO], name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Async version of upload_buffer."""
        # Convert BytesIO to bytes if needed
        if isinstance(data, io.BytesIO):
            data = data.getvalue()

        # Generate content-addressed key
        prefix = name.replace(".", "_") if name else "data"
        key = self._generate_key(data, prefix)

        # Upload using aioboto3
        session = aioboto3.Session()
        async with session.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=AKAVE_REGION,
        ) as s3:
            await s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
            )

        return {
            "Name": name or key,
            "Hash": key,
            "Size": str(len(data)),
        }

    def upload_json(
        self, data: Dict[str, Any], name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Upload JSON object to Akave.

        Args:
            data: Dictionary to upload as JSON
            name: Optional name

        Returns:
            {
                "Name": str,
                "Hash": str (object key),
                "Size": str
            }
        """
        json_str = json.dumps(data, sort_keys=True)
        return self.upload_text(json_str, name=name)

    async def upload_json_async(
        self, data: Dict[str, Any], name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Async version of upload_json."""
        json_str = json.dumps(data, sort_keys=True)
        return await self.upload_text_async(json_str, name=name)

    def retrieve(self, key: str) -> bytes:
        """
        Retrieve raw bytes from Akave.

        Args:
            key: Object key (returned as "Hash" from upload methods)

        Returns:
            Raw bytes
        """
        response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
        result: bytes = response["Body"].read()
        return result

    async def retrieve_async(self, key: str) -> bytes:
        """Async version of retrieve."""
        session = aioboto3.Session()
        async with session.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=AKAVE_REGION,
        ) as s3:
            response = await s3.get_object(Bucket=self.bucket, Key=key)
            async with response["Body"] as stream:
                result: bytes = await stream.read()
                return result

    def retrieve_text(self, key: str) -> str:
        """
        Retrieve text data from Akave.

        Args:
            key: Object key

        Returns:
            Text content as string
        """
        data = self.retrieve(key)
        return data.decode("utf-8")

    async def retrieve_text_async(self, key: str) -> str:
        """Async version of retrieve_text."""
        data = await self.retrieve_async(key)
        return data.decode("utf-8")

    def retrieve_json(self, key: str) -> Dict[str, Any]:
        """
        Retrieve JSON data from Akave.

        Args:
            key: Object key

        Returns:
            Parsed JSON as dictionary
        """
        text = self.retrieve_text(key)
        result: Dict[str, Any] = json.loads(text)
        return result

    async def retrieve_json_async(self, key: str) -> Dict[str, Any]:
        """Async version of retrieve_json."""
        text = await self.retrieve_text_async(key)
        result: Dict[str, Any] = json.loads(text)
        return result

    def upload_haystack_document(
        self, document: Any, as_text: bool = True
    ) -> Dict[str, Any]:
        """
        Upload Haystack Document to Akave.

        Args:
            document: Haystack Document object
            as_text: If True, upload content as text (more readable).
                     If False, upload as JSON with full metadata and embedding.

        Returns:
            Upload response with object key
        """
        # If document has embedding, always use JSON to preserve it
        has_embedding = (
            hasattr(document, "embedding") and document.embedding is not None
        )

        if as_text and not has_embedding:
            content = (
                document.content if hasattr(document, "content") else str(document)
            )
            doc_id = document.id if hasattr(document, "id") else "unknown"
            return self.upload_text(content, name=f"document_{doc_id}")
        else:
            # Upload as JSON with full metadata and embedding
            doc_dict = {
                "content": (
                    document.content if hasattr(document, "content") else str(document)
                ),
                "meta": document.meta if hasattr(document, "meta") else {},
                "id": document.id if hasattr(document, "id") else None,
            }
            # Include embedding if present (convert to list for JSON serialization)
            if has_embedding:
                embedding = document.embedding
                # Handle numpy arrays
                if hasattr(embedding, "tolist"):
                    embedding = embedding.tolist()
                doc_dict["embedding"] = embedding
            return self.upload_json(
                doc_dict, name=f"document_{doc_dict.get('id', 'unknown')}"
            )

    def upload_any(
        self, data: Any, name: Optional[str] = None, as_text: bool = True
    ) -> Dict[str, Any]:
        """
        Smart upload that handles any data type.

        Args:
            data: Any data (Document, dict, str, bytes, etc.)
            name: Optional name
            as_text: For Documents, upload as readable text (default True)

        Returns:
            Upload response with object key
        """
        # Handle Haystack Document
        if hasattr(data, "content"):
            return self.upload_haystack_document(data, as_text=as_text)

        # Handle bytes - skip large binary files
        if isinstance(data, bytes):
            if len(data) > 5 * 1024 * 1024:
                return {"Hash": "skipped_large_binary", "Size": str(len(data))}
            return self.upload_buffer(data, name=name)

        # Handle dict/list (JSON)
        if isinstance(data, dict):
            return self.upload_json(data, name=name)
        if isinstance(data, list):
            return self.upload_json({"data": data}, name=name)

        # Handle string
        if isinstance(data, str):
            return self.upload_text(data, name=name)

        # Fallback: convert to string
        return self.upload_text(str(data), name=name)

    async def upload_any_async(
        self, data: Any, name: Optional[str] = None, as_text: bool = True
    ) -> Dict[str, Any]:
        """Async version of upload_any."""
        # Handle Haystack Document
        if hasattr(data, "content"):
            return self.upload_haystack_document(data, as_text=as_text)

        # Handle bytes - skip large binary files
        if isinstance(data, bytes):
            if len(data) > 5 * 1024 * 1024:
                return {"Hash": "skipped_large_binary", "Size": str(len(data))}
            return await self.upload_buffer_async(data, name=name)

        # Handle dict/list (JSON)
        if isinstance(data, dict):
            return await self.upload_json_async(data, name=name)
        if isinstance(data, list):
            return await self.upload_json_async({"data": data}, name=name)

        # Handle string
        if isinstance(data, str):
            return await self.upload_text_async(data, name=name)

        # Fallback: convert to string
        return await self.upload_text_async(str(data), name=name)

    def delete(self, key: str) -> None:
        """
        Delete an object from Akave.

        Args:
            key: Object key to delete
        """
        self.s3_client.delete_object(Bucket=self.bucket, Key=key)

    async def delete_async(self, key: str) -> None:
        """Async version of delete."""
        session = aioboto3.Session()
        async with session.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=AKAVE_REGION,
        ) as s3:
            await s3.delete_object(Bucket=self.bucket, Key=key)
