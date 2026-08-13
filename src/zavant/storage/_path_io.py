"""Shared primitives for path-backed, content-addressed storage."""

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict
from uuid import uuid4

from zavant.storage.artifacts import ArtifactReference


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def canonical_json_sha256(payload: Dict[str, Any]) -> str:
    """Hash JSON independently of insignificant whitespace and key order."""
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return sha256_bytes(canonical)


def encode_json(payload: Dict[str, Any]) -> bytes:
    """Encode stable, human-readable UTF-8 JSON ending in a newline."""
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")


def read_json_object(path: Path) -> Dict[str, Any]:
    """Read a JSON object from a path-backed object.

    Args:
        path: File containing a JSON object.

    Returns:
        The parsed JSON object.

    Raises:
        ValueError: If the JSON root is not an object.
        OSError: If the file cannot be read.
        json.JSONDecodeError: If the file is not valid JSON.
    """

    payload = json.loads(path.read_bytes())
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def artifact_reference_for_path(
    storage_root: Path,
    artifact_path: Path,
) -> ArtifactReference:
    """Create a storage-neutral reference for an object below a storage root.

    Args:
        storage_root: Configured path-backed storage root.
        artifact_path: Persisted path below the storage root.

    Returns:
        Portable artifact key paired with its backend-specific URI.

    Raises:
        ValueError: If the artifact path is outside the storage root.
    """

    try:
        key = artifact_path.relative_to(storage_root).as_posix()
    except ValueError as exc:
        raise ValueError("artifact path must be under storage_root") from exc
    return ArtifactReference(key=key, uri=str(artifact_path))


def resolve_artifact_path(
    storage_root: Path,
    reference: ArtifactReference,
) -> Path:
    """Resolve and validate an artifact reference for a path-backed root.

    Args:
        storage_root: Configured path-backed storage root.
        reference: Storage-neutral reference created for this root.

    Returns:
        Backend path represented by the reference.

    Raises:
        ValueError: If the reference belongs to another storage root or backend.
    """

    artifact_path = storage_root.joinpath(*reference.key.split("/"))
    if reference.uri != str(artifact_path):
        raise ValueError("artifact reference does not belong to this storage root")
    return artifact_path


def atomic_write(destination: Path, content: bytes) -> None:
    """Atomically publish bytes at a destination path.

    Args:
        destination: Final path for the content.
        content: Bytes to persist.

    Raises:
        OSError: If the temporary or final write fails.
    """

    backend_write = getattr(destination, "atomic_write", None)
    if callable(backend_write):
        backend_write(content)
        return

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_bytes(content)
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()
