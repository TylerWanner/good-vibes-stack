"""MinIO storage client for private files (images, audio, etc).

Files are stored locally on-device. Never sent to cloud APIs.
Raw files stay here; only extracted text goes to Postgres.
"""
from __future__ import annotations

import io
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


class StorageClient:
    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        bucket: str | None = None,
    ) -> None:
        self.endpoint = endpoint or os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
        self.access_key = access_key or os.environ.get("MINIO_ROOT_USER", "second_brain")
        self.secret_key = secret_key or os.environ.get("MINIO_ROOT_PASSWORD", "change-me-minio")
        self.bucket = bucket or os.environ.get("MINIO_BUCKET", "second-brain-private")
        self._client = None

    def _get_client(self):
        if self._client is None:
            from minio import Minio
            from urllib.parse import urlparse
            parsed = urlparse(self.endpoint)
            host = parsed.netloc
            secure = parsed.scheme == "https"
            self._client = Minio(host, access_key=self.access_key, secret_key=self.secret_key, secure=secure)
            self._ensure_bucket()
        return self._client

    def _ensure_bucket(self) -> None:
        client = self._client
        if not client.bucket_exists(self.bucket):
            client.make_bucket(self.bucket)
            logger.info(f"Created MinIO bucket: {self.bucket}")

    def store_file(self, data: bytes, content_type: str, prefix: str = "uploads") -> str:
        """Store raw bytes in MinIO. Returns the object path."""
        ext = _ext_for_content_type(content_type)
        ts = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        import uuid
        object_name = f"{prefix}/{ts}/{uuid.uuid4().hex}{ext}"
        client = self._get_client()
        client.put_object(
            self.bucket,
            object_name,
            io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )
        logger.info(f"Stored {len(data)} bytes at {self.bucket}/{object_name}")
        return f"{self.bucket}/{object_name}"

    def get_file(self, file_path: str) -> bytes:
        """Retrieve file bytes by path (bucket/object_name)."""
        bucket, object_name = file_path.split("/", 1)
        client = self._get_client()
        response = client.get_object(bucket, object_name)
        try:
            return response.read()
        finally:
            response.close()


def _ext_for_content_type(content_type: str) -> str:
    mapping = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "audio/mpeg": ".mp3",
        "audio/ogg": ".ogg",
        "audio/wav": ".wav",
        "audio/mp4": ".m4a",
        "video/mp4": ".mp4",
        "application/pdf": ".pdf",
    }
    return mapping.get(content_type, ".bin")
