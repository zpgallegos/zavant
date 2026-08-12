# ADR 0020: Model player game data by team

- Status: Accepted
- Date: 2026-08-12

## Context

The v1 analytical contract keyed boxscore players and player statistics by
player within a game revision. MLB game 746942 violates that assumption: Danny
Jansen participated for Toronto before the game was suspended and for Boston
after it resumed following his trade. Both boxscore entries contain legitimate
game activity and cannot be collapsed without losing team attribution.

MLB responses can also retain an empty roster entry for a player's former club.
Choosing one entry with a heuristic makes common data projectable but cannot
represent the suspended-game case faithfully.

## Decision

Project every player-team boxscore entry. Key `players`, player positions, and
player batting, pitching, and fielding statistics by both player and team within
the revision-aware identity. Derive the team from the enclosing away or home
boxscore object rather than the optional player-level `parentTeamId`.

Include required `team_id` and `team_side` columns in the player identity,
position, batting, pitching, and fielding contracts from their first supported
version. The earlier experimental tables never completed a full projection and
had no downstream consumers, so discard those analytical tables and rebuild
them rather than preserving a non-working contract or calling the correction
v2.

## Consequences

Dual-team appearances and stale former-team roster entries are preserved as
separate facts instead of rejected or silently combined. Player-stat queries
must include team identity at their grain. The first working analytical
contract remains v1, without carrying experimental rows or compatibility
machinery forward.

## Alternatives considered

- Prefer the boxscore entry with more participation evidence. Rejected because
  both entries can be legitimate.
- Combine statistics across teams. Rejected because it destroys club-level
  attribution and can produce misleading totals.
- Migrate the experimental tables and publish contract v2. Rejected because v1
  never completed successfully and has no downstream consumers, so preserving
  it would create needless compatibility and query-filtering work.
