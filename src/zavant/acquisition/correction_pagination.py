"""Consistency checks for offset-paginated correction responses."""

from typing import Set

from zavant.contracts.game_changes import GameChangesResponse


class GameChangesPollingError(RuntimeError):
    """Raised when a correction poll cannot establish a complete page set."""


class CorrectionPaginationGuard:
    """Reject observable source mutations across one correction page scan."""

    def __init__(self, limit: int, max_pages: int, scope: str) -> None:
        self.limit = limit
        self.max_pages = max_pages
        self.scope = scope
        self.total_items = 0
        self.expected_page_count = 1
        self.next_page_number = 0
        self.game_pks: Set[int] = set()

    def accept(self, page_number: int, changes: GameChangesResponse) -> None:
        if page_number != self.next_page_number:
            raise AssertionError("correction pages must be observed in order")
        if page_number == 0:
            self.total_items = changes.total_items
            self.expected_page_count = max(
                1,
                (self.total_items + self.limit - 1) // self.limit,
            )
            if self.expected_page_count > self.max_pages:
                raise GameChangesPollingError(
                    f"{self.scope} requires {self.expected_page_count} pages, "
                    f"exceeding max_pages={self.max_pages}"
                )
        elif changes.total_items != self.total_items:
            raise GameChangesPollingError(
                f"{self.scope} totalItems changed from {self.total_items} to "
                f"{changes.total_items} while paging"
            )

        offset = page_number * self.limit
        expected_items = min(self.limit, max(0, self.total_items - offset))
        if len(changes.changed_games) != expected_items:
            raise GameChangesPollingError(
                f"{self.scope} page {page_number} contains "
                f"{len(changes.changed_games)} deduplicated games; expected "
                f"{expected_items}"
            )
        page_game_pks = {game.game_pk for game in changes.changed_games}
        repeated_game_pks = self.game_pks.intersection(page_game_pks)
        if repeated_game_pks:
            raise GameChangesPollingError(
                f"{self.scope} repeated gamePk values across pages while paging"
            )
        self.game_pks.update(page_game_pks)
        self.next_page_number += 1

    def validate_complete(self) -> None:
        if self.next_page_number != self.expected_page_count:
            raise AssertionError("correction page scan ended before its expected page count")
        if len(self.game_pks) != self.total_items:
            raise GameChangesPollingError(
                f"{self.scope} contains {len(self.game_pks)} unique games; expected "
                f"totalItems={self.total_items}"
            )
