"""Minimal conditional S3 object and path machinery for storage adapters."""

from dataclasses import dataclass
from datetime import datetime
from fnmatch import fnmatch
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, Optional, Protocol, Tuple, Union


class S3Client(Protocol):
    """Subset of the Boto3 S3 client used by Zavant."""

    def get_object(self, **kwargs: Any) -> Dict[str, Any]:
        ...

    def head_object(self, **kwargs: Any) -> Dict[str, Any]:
        ...

    def put_object(self, **kwargs: Any) -> Dict[str, Any]:
        ...

    def list_objects_v2(self, **kwargs: Any) -> Dict[str, Any]:
        ...


class S3ObjectWriteConflictError(OSError):
    """Raised when an S3 conditional write observes competing state."""


@dataclass(frozen=True)
class S3ObjectSummary:
    """Logical key and modification time returned by an S3 listing."""

    key: str
    last_modified: Optional[datetime]


class S3ObjectBackend:
    """Read, list, and conditionally write objects under one S3 prefix.

    Args:
        client: Boto3-compatible S3 client.
        bucket: S3 bucket name.
        prefix: Optional logical lake prefix within the bucket.

    Raises:
        ValueError: If the bucket or prefix is invalid.
    """

    def __init__(self, client: S3Client, bucket: str, prefix: str = "") -> None:
        if not bucket or "/" in bucket:
            raise ValueError("S3 bucket must be a non-empty bucket name")
        normalized_prefix = prefix.strip("/")
        if ".." in PurePosixPath(normalized_prefix).parts:
            raise ValueError("S3 prefix must not traverse parent keys")
        self.client = client
        self.bucket = bucket
        self.prefix = normalized_prefix
        self._observed_versions: Dict[str, Optional[str]] = {}

    def root(self) -> "S3Path":
        return S3Path(self, "")

    def uri(self, key: str = "") -> str:
        full_key = self._full_key(key)
        suffix = f"/{full_key}" if full_key else ""
        return f"s3://{self.bucket}{suffix}"

    def exists(self, key: str) -> bool:
        """Return whether a logical object exists and remember its version.

        Args:
            key: Logical key relative to this backend.

        Returns:
            `True` when S3 contains the object.

        Raises:
            OSError: If S3 returns an unexpected error.
        """

        try:
            response = self.client.head_object(
                Bucket=self.bucket,
                Key=self._full_key(key),
            )
        except Exception as exc:
            if self._is_missing(exc):
                self._observed_versions[key] = None
                return False
            raise OSError(f"failed to inspect S3 object {self.uri(key)}") from exc
        self._observed_versions[key] = self._etag(response)
        return True

    def read(self, key: str) -> bytes:
        """Read exact object bytes and remember the observed version.

        Args:
            key: Logical key relative to this backend.

        Returns:
            Exact stored bytes.

        Raises:
            FileNotFoundError: If the object does not exist.
            OSError: If S3 returns an unexpected error or body.
        """

        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._full_key(key),
            )
        except Exception as exc:
            if self._is_missing(exc):
                self._observed_versions[key] = None
                raise FileNotFoundError(self.uri(key)) from exc
            raise OSError(f"failed to read S3 object {self.uri(key)}") from exc
        body = response.get("Body")
        read = getattr(body, "read", None)
        if not callable(read):
            raise OSError(f"S3 object {self.uri(key)} has no readable body")
        close = getattr(body, "close", None)
        try:
            content = read()
        finally:
            if callable(close):
                close()
        if not isinstance(content, bytes):
            raise OSError(f"S3 object {self.uri(key)} did not return bytes")
        self._observed_versions[key] = self._etag(response)
        return content

    def write(self, key: str, content: bytes) -> None:
        """Publish bytes only when the last observed object version still applies.

        Args:
            key: Logical key relative to this backend.
            content: Exact bytes to publish.

        Raises:
            S3ObjectWriteConflictError: If another writer changed the object.
            OSError: If S3 returns another error.
        """

        if key not in self._observed_versions:
            self.exists(key)
        expected_version = self._observed_versions[key]
        request: Dict[str, Any] = {
            "Body": content,
            "Bucket": self.bucket,
            "ContentType": "application/json",
            "Key": self._full_key(key),
        }
        if expected_version is None:
            request["IfNoneMatch"] = "*"
        else:
            request["IfMatch"] = expected_version
        try:
            response = self.client.put_object(**request)
        except Exception as exc:
            if self._is_conditional_conflict(exc):
                try:
                    competing_content = self.read(key)
                except OSError:
                    competing_content = None
                if competing_content == content:
                    return
                raise S3ObjectWriteConflictError(
                    f"S3 object changed while writing {self.uri(key)}"
                ) from exc
            raise OSError(f"failed to write S3 object {self.uri(key)}") from exc
        self._observed_versions[key] = self._etag(response)

    def overwrite(self, key: str, content: bytes) -> None:
        """Publish bytes without coordinating with another writer.

        This is appropriate only for derived, replaceable objects whose writers do
        not need compare-and-swap semantics. State such as revision pointers and
        watermarks must use :meth:`write` instead.
        """

        request: Dict[str, Any] = {
            "Body": content,
            "Bucket": self.bucket,
            "ContentType": "application/json",
            "Key": self._full_key(key),
        }
        try:
            response = self.client.put_object(**request)
        except Exception as exc:
            raise OSError(f"failed to overwrite S3 object {self.uri(key)}") from exc
        self._observed_versions[key] = self._etag(response)

    def list(self, key_pattern: str) -> Tuple[str, ...]:
        """List logical keys matching a relative glob pattern.

        Args:
            key_pattern: POSIX-style key pattern containing optional `*` tokens.

        Returns:
            Sorted matching logical keys.

        Raises:
            OSError: If S3 listing fails or returns malformed pagination.
        """

        static_prefix = key_pattern.split("*", 1)[0]
        return tuple(
            summary.key
            for summary in self.list_objects(static_prefix)
            if fnmatch(summary.key, key_pattern)
        )

    def list_objects(self, key_prefix: str) -> Tuple[S3ObjectSummary, ...]:
        """List object keys and modification times below a logical prefix."""

        request_prefix = self._full_key(key_prefix)
        continuation_token: Optional[str] = None
        objects = []
        while True:
            request: Dict[str, Any] = {
                "Bucket": self.bucket,
                "Prefix": request_prefix,
            }
            if continuation_token is not None:
                request["ContinuationToken"] = continuation_token
            try:
                response = self.client.list_objects_v2(**request)
            except Exception as exc:
                raise OSError(
                    f"failed to list S3 objects below {self.uri(key_prefix)}"
                ) from exc
            for entry in self._contents(response):
                full_key = entry.get("Key")
                if not isinstance(full_key, str):
                    raise OSError("S3 listing contains an invalid key")
                logical_key = self._logical_key(full_key)
                last_modified = entry.get("LastModified")
                if last_modified is not None and not isinstance(
                    last_modified, datetime
                ):
                    raise OSError("S3 listing contains an invalid modification time")
                objects.append(S3ObjectSummary(logical_key, last_modified))
            if not response.get("IsTruncated", False):
                break
            token = response.get("NextContinuationToken")
            if not isinstance(token, str) or not token:
                raise OSError("truncated S3 listing has no continuation token")
            continuation_token = token
        return tuple(sorted(objects, key=lambda item: item.key))

    def _full_key(self, key: str) -> str:
        normalized_key = key.strip("/")
        if self.prefix and normalized_key:
            return f"{self.prefix}/{normalized_key}"
        return self.prefix or normalized_key

    def _logical_key(self, full_key: str) -> str:
        if not self.prefix:
            return full_key
        prefix = f"{self.prefix}/"
        if not full_key.startswith(prefix):
            raise OSError("S3 listing escaped the configured prefix")
        return full_key[len(prefix) :]

    @staticmethod
    def _contents(response: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        contents = response.get("Contents", [])
        if not isinstance(contents, list) or not all(
            isinstance(entry, dict) for entry in contents
        ):
            raise OSError("S3 listing contents are invalid")
        return contents

    @staticmethod
    def _etag(response: Dict[str, Any]) -> str:
        etag = response.get("ETag")
        if not isinstance(etag, str) or not etag:
            raise OSError("S3 response has no ETag")
        return etag

    @classmethod
    def _is_missing(cls, exc: Exception) -> bool:
        return cls._error_code(exc) in {"404", "NoSuchKey", "NotFound"}

    @classmethod
    def _is_conditional_conflict(cls, exc: Exception) -> bool:
        return cls._error_code(exc) in {
            "409",
            "412",
            "ConditionalRequestConflict",
            "PreconditionFailed",
        }

    @staticmethod
    def _error_code(exc: Exception) -> Optional[str]:
        response = getattr(exc, "response", None)
        if not isinstance(response, dict):
            return None
        error = response.get("Error")
        if not isinstance(error, dict):
            return None
        code = error.get("Code")
        return str(code) if code is not None else None


class S3Path:
    """Small path-like facade used by the shared persistence state machines."""

    def __init__(self, backend: S3ObjectBackend, key: str) -> None:
        self.backend = backend
        self.key = key.strip("/")

    def __truediv__(self, value: Union[str, Path]) -> "S3Path":
        value_text = value.as_posix() if isinstance(value, Path) else value
        if value_text.startswith("/") or ".." in PurePosixPath(value_text).parts:
            raise ValueError("S3 path segments must be safe and relative")
        key = f"{self.key}/{value_text}" if self.key else value_text
        return S3Path(self.backend, key)

    def joinpath(self, *values: str) -> "S3Path":
        result = self
        for value in values:
            result = result / value
        return result

    def exists(self) -> bool:
        return self.backend.exists(self.key)

    def read_bytes(self) -> bytes:
        return self.backend.read(self.key)

    def atomic_write(self, content: bytes) -> None:
        self.backend.write(self.key, content)

    def glob(self, pattern: str) -> Tuple["S3Path", ...]:
        full_pattern = f"{self.key}/{pattern}" if self.key else pattern
        return tuple(S3Path(self.backend, key) for key in self.backend.list(full_pattern))

    def relative_to(self, other: "S3Path") -> PurePosixPath:
        if self.backend is not other.backend:
            raise ValueError("S3 paths belong to different backends")
        try:
            return PurePosixPath(self.key).relative_to(PurePosixPath(other.key))
        except ValueError as exc:
            raise ValueError("S3 path is outside the requested root") from exc

    def __str__(self) -> str:
        return self.backend.uri(self.key)

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, S3Path):
            return NotImplemented
        return (self.backend.uri(), self.key) < (other.backend.uri(), other.key)
