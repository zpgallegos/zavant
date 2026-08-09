"""Shared primitives for local, atomic, content-addressed storage."""

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict
from uuid import uuid4


def sha256_bytes(content: bytes) -> str:
    """Calculate a SHA-256 digest for bytes.

    Args:
        content: Bytes to hash.

    Returns:
        The lowercase hexadecimal digest.
    """

    return hashlib.sha256(content).hexdigest()


def canonical_json_sha256(payload: Dict[str, Any]) -> str:
    """Calculate a stable digest for a parsed JSON object.

    Args:
        payload: Parsed JSON object to canonicalize.

    Returns:
        A SHA-256 digest unaffected by insignificant whitespace or key order.
    """

    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return sha256_bytes(canonical)


def encode_json(payload: Dict[str, Any]) -> bytes:
    """Encode an object as stable, human-readable JSON.

    Args:
        payload: JSON-serializable object.

    Returns:
        UTF-8 encoded JSON ending in a newline.
    """

    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")


def read_json_object(path: Path) -> Dict[str, Any]:
    """Read a JSON object from a local path.

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


def atomic_write(destination: Path, content: bytes) -> None:
    """Atomically publish bytes at a destination path.

    Args:
        destination: Final path for the content.
        content: Bytes to persist.

    Raises:
        OSError: If the temporary or final write fails.
    """

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_bytes(content)
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()
