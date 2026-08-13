"""Storage-neutral identities for persisted artifacts."""

from dataclasses import dataclass
from pathlib import PurePosixPath


@dataclass(frozen=True, order=True)
class ArtifactReference:
    """Identify one persisted artifact independently of its storage backend.

    Args:
        key: Portable path relative to the configured storage root.
        uri: Backend-specific location suitable for logs and operator output.

    Raises:
        ValueError: If the key or URI is empty, absolute, or unsafe.
    """

    key: str
    uri: str

    def __post_init__(self) -> None:
        if not self.key or "\\" in self.key:
            raise ValueError("artifact key must be a non-empty POSIX path")
        parsed_key = PurePosixPath(self.key)
        if parsed_key.is_absolute() or any(
            part in {"", ".", ".."} for part in parsed_key.parts
        ):
            raise ValueError("artifact key must be a safe relative POSIX path")
        if self.key != parsed_key.as_posix():
            raise ValueError("artifact key must use canonical POSIX separators")
        if not self.uri:
            raise ValueError("artifact uri must not be empty")

    def __str__(self) -> str:
        return self.uri
