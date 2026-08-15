# ADR 0023: Derive the initial player dimension from game evidence

- Status: Accepted
- Date: 2026-08-14

## Context

The first player-facing analytical product needs a one-row-per-player dimension
for names, biographical attributes, handedness, primary position, and recent
team context. The current game projection already exposes these attributes from
MLB's player objects in every boxscore.

MLB's season-level players resource can additionally return `currentTeam`, but
adopting it would require another source contract, raw landing convention,
daily acquisition branch, revision lifecycle, and non-game Glue projection.
That work would improve current affiliation in edge cases without changing the
player's calculated batting metrics.

Boxscore team identity has a different meaning: it identifies the club a player
represented in a particular game. A player's latest game team is usually their
current club, but it remains stale after a trade, release, retirement, or other
affiliation change until new game evidence appears. A player can also represent
two teams in one suspended game, as preserved under ADR 0020.

## Decision

Build the initial `dim_players` model as a Type 1 dimension from the most
chronologically recent row in `stg_boxscore_players`. Determine recency using
the game's resume date when present and otherwise its official date, followed
by its reported or scheduled start time and stable game identifier. Do not use
projection or source-observation timestamps because a correction to an older
game must not make that game the player's latest appearance.

Expose game-derived affiliation as `most_recent_game_team_id`, not
`current_team_id`. When the selected game contains player records for multiple
teams, publish a null team rather than choose one arbitrarily. Retain the game
identifier and dates supplying the profile so consumers can evaluate its
freshness and provenance.

Defer acquisition of the season-level players resource until authoritative
current affiliation, roster status, or profiles for players without recent game
evidence materially improve a product requirement.

## Consequences

The first player dimension reuses the existing current-game projection and adds
no acquisition, Glue, infrastructure, or operational failure surface. It is
sufficient for player metric pages and is straightforward to validate against
the underlying game data.

Profile attributes and team context can become stale when a player does not
appear in another acquired game. Consumers must not interpret
`most_recent_game_team_id` or the latest game active flag as authoritative
current roster state. A future current-player source can enrich the dimension
without changing historical game and team facts.

## Alternatives considered

- Acquire the current season's player resource daily and project every response
  revision. Deferred because its primary immediate benefit is more accurate
  current affiliation, while its acquisition and projection lifecycle is
  material relative to that benefit.
- Label the latest boxscore team as `current_team_id`. Rejected because that
  promises semantics the source does not provide.
- Resolve a dual-team latest game with a deterministic team sort. Rejected
  because determinism would conceal rather than resolve the source ambiguity.
