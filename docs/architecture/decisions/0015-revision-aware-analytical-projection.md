# ADR 0015: Project explicit revision-aware analytical datasets

- Status: Accepted
- Date: 2026-08-10

## Context

The legacy pipeline flattened each landed JSON document into seven wide datasets,
then appended files to Parquet based on filename tracking. Pitch attributes made
the common play-event dataset sparse, source corrections had no coherent
cross-table publication boundary, and generic flattening made schema changes
difficult to distinguish from deliberate mappings.

The current raw layer retains immutable content-addressed revisions and an
explicit pointer to the current revision. Analytical publication needs to keep
that provenance, remain testable without AWS, and eventually produce tables
that Athena and dbt can query efficiently.

## Decision

Define explicit table contracts and natural keys for each analytical grain. The
local projection contains:

- game context: `games`, `game_teams`, `innings`, `game_officials`, and
  `game_decisions`;
- play context: `plays` and a skinny `play_events` spine;
- sparse event families: `pitches`, `batted_balls`, `actions`, `substitutions`,
  `disengagements`, `non_pitch_calls`, `reviews`, and `rule_violations`;
- runner detail: `runner_movements` and `fielding_credits`; and
- boxscore context: `players`, `player_positions`, player batting, pitching, and
  fielding statistics, plus the corresponding three team-statistic tables.

Pitches are separated from other event families; batted-ball attributes are a
further optional extension of a pitch. Player tables contain game statistics,
not the mutable season-to-date snapshots embedded in each response. Every row
includes the source revision ID and projection contract version.

Preserve MLB's precomputed rate values such as batting average, OPS, ERA, WHIP,
and `inningsPitched` as informational source strings. Treat integer counting
statistics—including hits, at-bats, walks, total bases, earned runs, and outs—as
the canonical analytical measures. Aggregated rates must be derived from summed
counts; in particular, pitched innings are represented canonically by outs
rather than interpreting baseball notation such as `5.2` as a decimal.

Implement the source-to-row mapping as a pure Python projection core. A local
adapter discovers only revisions selected by persisted `current.json` pointers,
validates their content hashes and metadata, and atomically publishes explicit
Parquet schemas plus samples and a run manifest. Production Glue and Iceberg
composition will reuse the projection rules while replacing local discovery and
publication.

Production analytical tables will use Apache Iceberg format version 2. A daily
Step Functions Standard workflow will sequence the existing acquisition Lambda,
one batch Glue projection job, and later dbt execution. Projection is batched
rather than triggered for each S3 object so backfills and corrections do not
create concurrent table commits or excessive small files.

## Consequences

Adding a field or dataset requires an intentional contract change. This is more
work than recursive flattening but makes nullability, types, keys, and schema
evolution reviewable. Unknown event families remain visible in the event spine
and run counts even before a dedicated extension exists.

Local Parquet output is a run snapshot for inspection and testing, not an
emulation of Iceberg transaction semantics. Later production publication must
add idempotent table merges and a completed-revision registry before current
views consume a corrected revision.
