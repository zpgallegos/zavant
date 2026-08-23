"""Path-backed storage for immutable game-change pages and poll manifests."""

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple
from uuid import UUID

from zavant._time import Clock, as_utc, utc_now
from zavant.ingestion.mlb_stats_api.contracts.game_changes import GameChangesRequest, GameChangesResponse
from zavant.storage._path_io import (
    atomic_write,
    encode_json,
    resolve_artifact_path,
    artifact_reference_for_path,
    read_json_object,
    sha256_bytes,
)
from zavant.ingestion.mlb_stats_api.storage._processing_outcomes import apply_processing_outcome
from zavant.storage.artifacts import ArtifactReference
from zavant.ingestion.mlb_stats_api.storage.errors import GameChangesConflictError
from zavant.ingestion.mlb_stats_api.storage.models import ChangedGameWorkItem, LandedGameChangesPage


GAME_CHANGE_PROCESSING_STATUSES = ("pending", "skipped", "succeeded", "failed")
GAME_CHANGE_PROCESSING_OUTCOMES = GAME_CHANGE_PROCESSING_STATUSES[1:]


class PathGameChangesStore:
    """Persist correction-feed pages and merge their routing manifest.

    Args:
        storage_root: Root path under which raw objects are stored.
        clock: Function returning the current timezone-aware UTC time.
    """

    def __init__(self, storage_root: Path, clock: Clock = utc_now) -> None:
        self.storage_root = storage_root
        self.clock = clock

    def land_page(
        self,
        changes: GameChangesResponse,
        request: GameChangesRequest,
        raw: bytes,
        run_id: UUID,
    ) -> LandedGameChangesPage:
        poll_date = as_utc(request.window_end, "window_end").date().isoformat()
        run_directory = (
            self.storage_root
            / "raw"
            / "mlb_stats_api"
            / "game_changes"
            / f"poll_date={poll_date}"
            / f"run_id={run_id}"
        )
        page_directory = run_directory / f"page={request.page_number:04d}"
        response_path = page_directory / "response.json"
        metadata_path = page_directory / "metadata.json"
        manifest_path = run_directory / "manifest.json"
        response_checksum = sha256_bytes(raw)
        observed_at = as_utc(self.clock(), "clock result")
        manifest = self._load_or_create_manifest(
            manifest_path=manifest_path,
            request=request,
            run_id=run_id,
            observed_at=observed_at,
        )

        created = not response_path.exists()
        if response_path.exists():
            existing_checksum = sha256_bytes(response_path.read_bytes())
            if existing_checksum != response_checksum:
                raise GameChangesConflictError(
                    f"poll page {request.page_number} already has different content"
                )
        else:
            atomic_write(response_path, raw)

        page_metadata = {
            "contract": "mlb-stats-api-game-changes-page/v1",
            "observed_at": observed_at.isoformat(),
            "request": request.as_dict(),
            "response_sha256": response_checksum,
            "run_id": str(run_id),
            "total_games": changes.total_games,
            "total_items": changes.total_items,
        }
        if metadata_path.exists():
            try:
                existing_metadata = read_json_object(metadata_path)
            except (ValueError, json.JSONDecodeError) as exc:
                raise GameChangesConflictError("poll page metadata is invalid") from exc
            immutable_fields = (
                "contract",
                "request",
                "response_sha256",
                "run_id",
                "total_games",
                "total_items",
            )
            if any(
                existing_metadata.get(key) != page_metadata[key]
                for key in immutable_fields
            ):
                raise GameChangesConflictError(
                    f"poll page {request.page_number} metadata conflicts"
                )
        else:
            atomic_write(metadata_path, encode_json(page_metadata))
        manifest_changed = self._merge_page(
            manifest=manifest,
            changes=changes,
            request=request,
            response_path=response_path,
            metadata_path=metadata_path,
            response_checksum=response_checksum,
            observed_at=observed_at,
        )
        if manifest_changed or not manifest_path.exists():
            atomic_write(manifest_path, encode_json(manifest))

        return LandedGameChangesPage(
            run_id=run_id,
            poll_date=poll_date,
            page_number=request.page_number,
            response_path=artifact_reference_for_path(self.storage_root, response_path),
            metadata_path=artifact_reference_for_path(self.storage_root, metadata_path),
            manifest_path=artifact_reference_for_path(self.storage_root, manifest_path),
            response_sha256=response_checksum,
            changed_game_pks=changes.game_pks,
            created=created,
        )

    def finalize_manifest(
        self,
        manifest_path: ArtifactReference,
        expected_page_count: int,
        expected_total_items: int,
        watermark_before: datetime,
    ) -> Dict[str, int]:
        if type(expected_page_count) is not int or expected_page_count <= 0:
            raise ValueError("expected_page_count must be a positive integer")
        if type(expected_total_items) is not int or expected_total_items < 0:
            raise ValueError("expected_total_items must be a non-negative integer")
        if watermark_before.tzinfo is None or watermark_before.utcoffset() is None:
            raise ValueError("watermark_before must include a UTC offset")

        resolved_manifest_path = resolve_artifact_path(self.storage_root, manifest_path)
        try:
            manifest = read_json_object(resolved_manifest_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise GameChangesConflictError("poll manifest is invalid") from exc

        pages_value = manifest.get("pages")
        if not isinstance(pages_value, list):
            raise GameChangesConflictError("poll manifest collections are invalid")
        pages = self._validated_pages(pages_value, expected_page_count)
        changed_games = self._validated_changed_games(manifest)

        first_total_items = pages[0].get("total_items")
        if first_total_items != expected_total_items:
            raise GameChangesConflictError(
                "poll manifest first page has a conflicting total_items"
            )

        updated_since = self._manifest_timestamp(manifest, "updated_since")
        window_end = self._manifest_timestamp(manifest, "window_end")
        normalized_watermark = as_utc(watermark_before, "watermark_before")
        if updated_since > normalized_watermark:
            raise GameChangesConflictError(
                "poll query boundary is after its logical watermark"
            )
        if normalized_watermark >= window_end:
            raise GameChangesConflictError(
                "poll watermark must be before its window end"
            )

        summary = {
            "changed_games": len(changed_games),
            "pages": len(pages),
            "source_items": expected_total_items,
        }
        processing_summary = self._processing_summary(changed_games)
        processing_status = self._processing_status(processing_summary)
        completed_at = as_utc(self.clock(), "clock result").isoformat()
        normalized_watermark_text = normalized_watermark.isoformat()
        if manifest.get("status") == "complete":
            expected_completion = {
                "processing_status": processing_status,
                "processing_summary": processing_summary,
                "summary": summary,
                "watermark_before": normalized_watermark_text,
            }
            if any(
                manifest.get(key) != value for key, value in expected_completion.items()
            ):
                raise GameChangesConflictError(
                    "completed poll manifest conflicts with finalization"
                )
            return summary
        if manifest.get("status") != "open":
            raise GameChangesConflictError("poll manifest has an invalid status")

        manifest["completed_at"] = completed_at
        manifest["processing_status"] = processing_status
        manifest["processing_summary"] = processing_summary
        manifest["status"] = "complete"
        manifest["summary"] = summary
        manifest["updated_at"] = completed_at
        manifest["watermark_before"] = normalized_watermark_text
        atomic_write(resolved_manifest_path, encode_json(manifest))
        return summary

    def processable_manifests(self) -> Tuple[ArtifactReference, ...]:
        pattern = "raw/mlb_stats_api/game_changes/poll_date=*/run_id=*/manifest.json"
        processable: List[ArtifactReference] = []
        for manifest_path in sorted(self.storage_root.glob(pattern)):
            manifest = self._read_manifest(manifest_path)
            if manifest.get("status") != "complete":
                continue
            games = self._validated_changed_games(manifest)
            if any(
                game["processing_status"] in {"pending", "failed"} for game in games
            ):
                processable.append(
                    artifact_reference_for_path(self.storage_root, manifest_path)
                )
        return tuple(processable)

    def game_work_items(
        self, manifest_path: ArtifactReference
    ) -> Tuple[ChangedGameWorkItem, ...]:
        manifest = self._read_manifest(
            resolve_artifact_path(self.storage_root, manifest_path)
        )
        if manifest.get("status") != "complete":
            raise GameChangesConflictError(
                "changed games can be processed only from a complete poll"
            )
        games = self._validated_changed_games(manifest)
        return tuple(
            ChangedGameWorkItem(
                game_pk=game["game_pk"],
                season=game["season"],
                live_feed_link=game["live_feed_link"],
                processing_status=game["processing_status"],
            )
            for game in games
            if game["processing_status"] in {"pending", "failed"}
        )

    def record_game_outcome(
        self,
        manifest_path: ArtifactReference,
        game_pk: int,
        status: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if status not in GAME_CHANGE_PROCESSING_OUTCOMES:
            raise ValueError(f"unsupported changed-game outcome: {status}")
        resolved_manifest_path = resolve_artifact_path(self.storage_root, manifest_path)
        manifest = self._read_manifest(resolved_manifest_path)
        if manifest.get("status") != "complete":
            raise GameChangesConflictError(
                "changed games can be updated only in a complete poll"
            )
        games = self._validated_changed_games(manifest)
        matching_games = [game for game in games if game["game_pk"] == game_pk]
        if len(matching_games) != 1:
            raise GameChangesConflictError(
                f"poll manifest does not contain exactly one game {game_pk}"
            )

        game = matching_games[0]
        recorded_at = as_utc(self.clock(), "clock result").isoformat()
        attempts = game.get("processing_attempts", [])
        if not isinstance(attempts, list):
            raise GameChangesConflictError(
                f"poll manifest game {game_pk} has invalid processing attempts"
            )
        apply_processing_outcome(game, status, details, recorded_at)
        processing_summary = self._processing_summary(games)
        manifest["processing_status"] = self._processing_status(processing_summary)
        manifest["processing_summary"] = processing_summary
        manifest["updated_at"] = recorded_at
        atomic_write(resolved_manifest_path, encode_json(manifest))

    def finalize_processing(self, manifest_path: ArtifactReference) -> Dict[str, int]:
        resolved_manifest_path = resolve_artifact_path(self.storage_root, manifest_path)
        manifest = self._read_manifest(resolved_manifest_path)
        if manifest.get("status") != "complete":
            raise GameChangesConflictError(
                "changed games can be finalized only in a complete poll"
            )
        games = self._validated_changed_games(manifest)
        summary = self._processing_summary(games)
        processing_status = self._processing_status(summary)
        if (
            manifest.get("processing_status") != processing_status
            or manifest.get("processing_summary") != summary
        ):
            updated_at = as_utc(self.clock(), "clock result").isoformat()
            manifest["processing_status"] = processing_status
            manifest["processing_summary"] = summary
            manifest["updated_at"] = updated_at
            if processing_status == "complete":
                manifest["processing_completed_at"] = updated_at
            else:
                manifest.pop("processing_completed_at", None)
            atomic_write(resolved_manifest_path, encode_json(manifest))
        return summary

    @staticmethod
    def _validated_pages(
        page_values: List[Any],
        expected_page_count: int,
    ) -> List[Dict[str, Any]]:
        if len(page_values) != expected_page_count:
            raise GameChangesConflictError(
                "poll manifest does not contain every expected page"
            )
        pages: List[Dict[str, Any]] = []
        expected_offset = 0
        for expected_page_number, value in enumerate(page_values):
            if not isinstance(value, dict):
                raise GameChangesConflictError("poll manifest page is invalid")
            if value.get("page_number") != expected_page_number:
                raise GameChangesConflictError("poll manifest pages are not contiguous")
            limit = value.get("limit")
            offset = value.get("offset")
            if type(limit) is not int or limit <= 0 or offset != expected_offset:
                raise GameChangesConflictError(
                    "poll manifest page pagination is invalid"
                )
            pages.append(value)
            expected_offset += limit
        return pages

    @staticmethod
    def _manifest_timestamp(manifest: Dict[str, Any], key: str) -> datetime:
        value = manifest.get(key)
        if not isinstance(value, str):
            raise GameChangesConflictError(f"poll manifest {key} is invalid")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise GameChangesConflictError(f"poll manifest {key} is invalid") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise GameChangesConflictError(f"poll manifest {key} is invalid")
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _read_manifest(manifest_path: Path) -> Dict[str, Any]:
        try:
            return read_json_object(manifest_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise GameChangesConflictError("poll manifest is invalid") from exc

    @staticmethod
    def _validated_changed_games(
        manifest: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], ...]:
        games_value = manifest.get("changed_games")
        if not isinstance(games_value, list):
            raise GameChangesConflictError("poll manifest changed games are invalid")
        games: List[Dict[str, Any]] = []
        game_pks: Set[int] = set()
        for value in games_value:
            if not isinstance(value, dict):
                raise GameChangesConflictError("poll manifest changed game is invalid")
            game_pk = value.get("game_pk")
            season = value.get("season")
            live_feed_link = value.get("live_feed_link")
            status = value.get("processing_status")
            if type(game_pk) is not int or game_pk in game_pks:
                raise GameChangesConflictError(
                    "poll manifest changed-game identifiers are invalid"
                )
            if type(season) is not int or season <= 0:
                raise GameChangesConflictError(
                    f"poll manifest game {game_pk} has an invalid season"
                )
            if not isinstance(live_feed_link, str) or not live_feed_link:
                raise GameChangesConflictError(
                    f"poll manifest game {game_pk} has an invalid live-feed link"
                )
            if (
                not isinstance(status, str)
                or status not in GAME_CHANGE_PROCESSING_STATUSES
            ):
                raise GameChangesConflictError(
                    f"poll manifest game {game_pk} has an invalid processing status"
                )
            game_pks.add(game_pk)
            games.append(value)
        return tuple(games)

    @staticmethod
    def _processing_summary(
        games: Tuple[Dict[str, Any], ...],
    ) -> Dict[str, int]:
        summary = {status: 0 for status in GAME_CHANGE_PROCESSING_STATUSES}
        for game in games:
            summary[game["processing_status"]] += 1
        return summary

    @staticmethod
    def _processing_status(summary: Dict[str, int]) -> str:
        if summary["pending"]:
            return "pending"
        if summary["failed"]:
            return "failed"
        return "complete"

    def _load_or_create_manifest(
        self,
        manifest_path: Path,
        request: GameChangesRequest,
        run_id: UUID,
        observed_at: datetime,
    ) -> Dict[str, Any]:
        normalized_request = request.as_dict()
        if not manifest_path.exists():
            return {
                "changed_games": [],
                "contract": "mlb-stats-api-game-changes-manifest/v1",
                "created_at": observed_at.isoformat(),
                "pages": [],
                "processing_status": "pending",
                "run_id": str(run_id),
                "status": "open",
                "updated_at": observed_at.isoformat(),
                "updated_since": normalized_request["updated_since"],
                "window_end": normalized_request["window_end"],
            }

        try:
            manifest = read_json_object(manifest_path)
        except (ValueError, json.JSONDecodeError) as exc:
            raise GameChangesConflictError("poll manifest is invalid") from exc

        expected = {
            "run_id": str(run_id),
            "updated_since": normalized_request["updated_since"],
            "window_end": normalized_request["window_end"],
        }
        for key, expected_value in expected.items():
            if manifest.get(key) != expected_value:
                raise GameChangesConflictError(f"poll manifest has a conflicting {key}")

        if not isinstance(manifest.get("pages"), list) or not isinstance(
            manifest.get("changed_games"), list
        ):
            raise GameChangesConflictError("poll manifest collections are invalid")
        self._validated_changed_games(manifest)
        if manifest.get("status") not in {"open", "complete"}:
            raise GameChangesConflictError("poll manifest has an invalid status")
        return manifest

    def _merge_page(
        self,
        manifest: Dict[str, Any],
        changes: GameChangesResponse,
        request: GameChangesRequest,
        response_path: Path,
        metadata_path: Path,
        response_checksum: str,
        observed_at: datetime,
    ) -> bool:
        pages = manifest["pages"]
        assert isinstance(pages, list)
        existing_page: Optional[Dict[str, Any]] = None
        for value in pages:
            if (
                isinstance(value, dict)
                and value.get("page_number") == request.page_number
            ):
                existing_page = value
                break

        if existing_page is not None:
            if existing_page.get("response_sha256") != response_checksum:
                raise GameChangesConflictError(
                    f"poll manifest page {request.page_number} has a different response"
                )
            return False
        if manifest.get("status") == "complete":
            raise GameChangesConflictError(
                "completed poll manifest cannot accept another page"
            )

        page_entry = {
            "limit": request.limit,
            "metadata_path": self._relative_path(metadata_path),
            "offset": request.offset,
            "page_number": request.page_number,
            "response_path": self._relative_path(response_path),
            "response_sha256": response_checksum,
            "total_games": changes.total_games,
            "total_items": changes.total_items,
        }
        pages.append(page_entry)
        pages.sort(key=lambda value: value.get("page_number", -1))

        changed_games = manifest["changed_games"]
        assert isinstance(changed_games, list)
        games_by_pk: Dict[int, Dict[str, Any]] = {}
        for value in changed_games:
            if isinstance(value, dict) and type(value.get("game_pk")) is int:
                games_by_pk[value["game_pk"]] = value
        for changed_game in changes.changed_games:
            prior = games_by_pk.get(changed_game.game_pk)
            game_entry = changed_game.as_dict()
            if prior is not None:
                prior_status = prior.get("processing_status")
                if isinstance(prior_status, str):
                    game_entry["processing_status"] = prior_status
            games_by_pk[changed_game.game_pk] = game_entry
        manifest["changed_games"] = [
            games_by_pk[game_pk] for game_pk in sorted(games_by_pk)
        ]
        manifest["updated_at"] = observed_at.isoformat()
        return True

    def _relative_path(self, path: Path) -> str:
        return path.relative_to(self.storage_root).as_posix()
