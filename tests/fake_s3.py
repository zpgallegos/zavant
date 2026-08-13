"""Small in-memory S3 client used by storage and Lambda tests."""

from hashlib import sha256
from io import BytesIO
from typing import Any, Dict, Optional, Tuple


class FakeS3Error(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}
        super().__init__(code)


class FakeS3Client:
    def __init__(self, page_size: int = 1000) -> None:
        self.objects: Dict[Tuple[str, str], bytes] = {}
        self.page_size = page_size

    def get_object(self, **kwargs: Any) -> Dict[str, Any]:
        identity = self._identity(kwargs)
        try:
            content = self.objects[identity]
        except KeyError as exc:
            raise FakeS3Error("NoSuchKey") from exc
        return {"Body": BytesIO(content), "ETag": self._etag(content)}

    def head_object(self, **kwargs: Any) -> Dict[str, Any]:
        identity = self._identity(kwargs)
        try:
            content = self.objects[identity]
        except KeyError as exc:
            raise FakeS3Error("404") from exc
        return {"ContentLength": len(content), "ETag": self._etag(content)}

    def put_object(self, **kwargs: Any) -> Dict[str, Any]:
        identity = self._identity(kwargs)
        current = self.objects.get(identity)
        if kwargs.get("IfNoneMatch") == "*" and current is not None:
            raise FakeS3Error("PreconditionFailed")
        expected = kwargs.get("IfMatch")
        if expected is not None and (
            current is None or expected != self._etag(current)
        ):
            raise FakeS3Error("PreconditionFailed")
        body = kwargs.get("Body")
        if not isinstance(body, bytes):
            raise TypeError("Body must be bytes")
        self.objects[identity] = body
        return {"ETag": self._etag(body)}

    def list_objects_v2(self, **kwargs: Any) -> Dict[str, Any]:
        bucket = kwargs.get("Bucket")
        prefix = kwargs.get("Prefix", "")
        token = kwargs.get("ContinuationToken")
        if not isinstance(bucket, str) or not isinstance(prefix, str):
            raise TypeError("Bucket and Prefix must be strings")
        offset = int(token) if isinstance(token, str) else 0
        keys = sorted(
            key
            for object_bucket, key in self.objects
            if object_bucket == bucket and key.startswith(prefix)
        )
        page = keys[offset : offset + self.page_size]
        next_offset = offset + len(page)
        truncated = next_offset < len(keys)
        response: Dict[str, Any] = {
            "Contents": [{"Key": key} for key in page],
            "IsTruncated": truncated,
        }
        if truncated:
            response["NextContinuationToken"] = str(next_offset)
        return response

    @staticmethod
    def _identity(kwargs: Dict[str, Any]) -> Tuple[str, str]:
        bucket = kwargs.get("Bucket")
        key = kwargs.get("Key")
        if not isinstance(bucket, str) or not isinstance(key, str):
            raise TypeError("Bucket and Key must be strings")
        return bucket, key

    @staticmethod
    def _etag(content: bytes) -> str:
        return f'"{sha256(content).hexdigest()}"'

    def content(self, bucket: str, key: str) -> Optional[bytes]:
        return self.objects.get((bucket, key))
