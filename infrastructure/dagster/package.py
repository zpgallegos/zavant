"""Build an allowlisted EC2 release, excluding local profiles and runtime state."""

import hashlib
import tarfile
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def release_files(root: Path) -> list[Path]:
    """Select source/configuration only; reject symlinks in the release inputs."""

    files = [root / name for name in ("pyproject.toml", "constraints.txt", "readme.md")]
    for directory, suffixes in (
        ("src", {".py"}),
        ("dbt", {".sql", ".yml", ".yaml", ".csv"}),
        ("infrastructure/dagster", {".yaml", ".yml", ".sh", ".service"}),
    ):
        for path in (root / directory).rglob("*"):
            if {"target", "logs", "dbt_packages", "__pycache__"} & set(path.parts):
                continue
            if directory == "dbt" and path.name == "profiles.yml":
                continue
            if path.is_file() and path.suffix in suffixes:
                files.append(path)
    for path in files:
        if any(
            part.is_symlink()
            for part in (path, *path.parents)
            if part.is_relative_to(root)
        ):
            raise ValueError(f"Release inputs must not be symlinks: {path}")
        if not path.resolve().is_relative_to(root.resolve()):
            raise ValueError(f"Release input escapes repository: {path}")
    return sorted(files)


def main() -> None:
    """Create a local archive and print its checksum; nothing is uploaded."""

    archive_path = REPOSITORY_ROOT / "build" / "zavant-dagster.tar.gz"
    archive_path.parent.mkdir(exist_ok=True)
    with tarfile.open(archive_path, "w:gz") as archive:
        for path in release_files(REPOSITORY_ROOT):
            archive.add(
                path, arcname=path.relative_to(REPOSITORY_ROOT), recursive=False
            )
    print(f"{hashlib.sha256(archive_path.read_bytes()).hexdigest()}  {archive_path}")


if __name__ == "__main__":
    main()
