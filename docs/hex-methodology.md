# Zavant player-profile methodology

This document is the source-controlled companion to the Hex player profile. It
separates measurements supplied by MLB from transformations and metrics derived
by Zavant, documents important denominator choices, and provides a concise
methodology page for the published analytical product.

[Return to the project overview](../readme.md) ·
[Review the semantic layer](../dbt/) ·
[Inspect the data platform](data-platform.md) ·
[Open the published Hex player profile](https://app.hex.tech/01a00124-662e-7369-982a-ba58e4f2a22f/app/0347rXGBaRqd4gD8KHxHRr/latest)

## What the product shows

The player profile combines traditional batting results with batted-ball contact
quality. Player and season filters apply to both semantic models, allowing the
same selection to drive counting statistics, rate metrics, contact measurements,
and batted-ball distributions.

The source-controlled semantic layer also includes pitch, runner-movement, and
player-game participation grains. Those support complete pitch counts,
pitch-family and count splits, baserunning totals, and games played without
forcing unrelated events into the plate-appearance grain. The published app's
current primary experience remains the batting and contact-quality profile.

The current portfolio includes final regular-season MLB games. Postseason,
Spring Training, exhibition, unfinished, and cancelled games are not included in
the published batting population.

## Data lineage

```mermaid
flowchart LR
    api[MLB Stats API live game feed]
    raw[Immutable revisioned JSON]
    projection[Grain-specific Python projection]
    iceberg[Current Iceberg tables]
    facts[dbt facts and dimensions]
    semantics[MetricFlow measures and metrics]
    profile[Hex player profile]

    api --> raw
    raw --> projection
    projection --> iceberg
    iceberg --> facts
    facts --> semantics
    semantics --> profile
```

Every published fact retains the source game revision from which it was built.
Glue resolves the current revision, dbt replaces complete games when that
revision changes, and MetricFlow aggregates the resulting current-state facts.

## MLB-supplied observations

MLB supplies the underlying event evidence, including:

- game and participant identifiers;
- official plate-appearance result classifications;
- pitch and batted-ball event sequence;
- official boxscore sections retained separately for reconciliation;
- exit velocity, launch angle, estimated distance, trajectory, and other
  tracking values when available; and
- corrected complete-game responses discovered after initial publication.

Zavant does not claim to independently measure exit velocity, launch angle, or
official scoring outcomes. Those are attributed to MLB.

## Zavant-derived analytical logic

Zavant derives the reusable analytical product from those observations:

- qualification of MLB's broader `allPlays` stream into official plate
  appearances and at-bats;
- deterministic player, team, game, plate-appearance, and batted-ball keys;
- direct pitch-event qualification, pre-pitch count state, and governed
  fastball, breaking, offspeed, and other pitch-family classifications;
- hit, total-base, walk, strikeout, sacrifice, hard-hit, sweet-spot, and tracking
  eligibility indicators;
- game-state and matchup dimensions;
- additive measures that remain valid when regrouped; and
- ratios and derived metrics governed in MetricFlow rather than rewritten in
  individual Hex charts.

## Metric definitions

| Display metric | Definition |
|---|---|
| Plate appearances | Count of qualified completed plate appearances. |
| At-bats | Sum of outcomes charged as official at-bats. |
| AVG | Hits divided by official at-bats. |
| OBP | Hits, walks, and hit-by-pitch divided by at-bats, walks, hit-by-pitch, and sacrifice flies. |
| SLG | Total bases divided by official at-bats. |
| OPS | On-base percentage plus slugging percentage. |
| Batted-ball events | Count of projected batted-ball events. |
| Average exit velocity | Sum of measured exit velocities divided by events with an exit-velocity observation. |
| Maximum exit velocity | Highest supplied exit velocity in the selected population. |
| Average launch angle | Sum of measured non-bunt launch angles divided by non-bunt events with a launch-angle observation. |
| Hard-hit rate | Events hit at least 95 mph divided by events with measured exit velocity. |
| Sweet-spot rate | Events with launch angle from 8 through 32 degrees divided by events with measured launch angle. |
| Pitches | Count of actual pitch events, including pitches in plays that do not end in a completed plate appearance. |
| Pitch-family rate | Pitches in a governed pitch family divided by all actual pitches in the selected population. |
| Average release velocity | Sum of supplied release velocities divided by pitches with a release-velocity observation. |

The source of truth for the complete definitions is
[`metrics_plate_appearances.yml`](../dbt/models/semantic/plate_appearances/metrics_plate_appearances.yml)
and
[`metrics_batted_balls.yml`](../dbt/models/semantic/batted_balls/metrics_batted_balls.yml).
Pitch definitions live in
[`metrics_pitches.yml`](../dbt/models/semantic/pitches/metrics_pitches.yml).

## Why rates use aggregate components

A player-season average cannot safely be averaged again across months, teams,
or other groups. Zavant therefore stores additive numerators and denominators,
then asks MetricFlow to divide their aggregate values at the requested query
grain.

For example:

```text
AVG = sum(hit_ind) / sum(at_bat_ind)
```

This produces the same definition for one game, one player-season, a team, or
the entire retained population. Contact averages follow the same pattern by
dividing the sum of tracked measurements by the number of eligible observations.

## Tracking eligibility

MLB does not supply every tracking measurement for every batted ball. Missing
measurements are not converted to zero:

- Average exit velocity and hard-hit rate use only events with exit velocity.
- Average launch angle excludes bunts and requires launch angle.
- Sweet-spot rate requires launch angle.
- Statcast tracking rate reports the share of batted balls with both primary
  contact measurements.

The profile can therefore distinguish performance from measurement coverage.

## Validation

The published metrics are supported by several independent checks:

- Fact grains and required join keys are tested in dbt.
- Plate-appearance and at-bat counts reconcile to MLB's separately projected
  game boxscores.
- Player-game batting totals derived from play events reconcile to boxscore
  player batting lines.
- Relationship tests connect batted balls to pitch events and preserve other
  nested event grains.
- Current-revision tests confirm that incremental facts use the game revision
  selected by Glue.
- Pitch reconciliation verifies that the pitch fact preserves the complete
  actual-pitch staging population rather than only pitches attached to completed
  plate appearances.
- Warehouse-completeness reporting compares raw games with projected datasets
  by season.

These checks do not make MLB's source data independently authoritative, but they
do verify that Zavant's transformations are complete, internally consistent,
and traceable to retained source evidence.

## Current limitations

- The profile does not yet calculate barrels, expected statistics, percentile
  rankings, swing decisions, or fielding value.
- Governed pitch and baserunning models exist, but the published profile's
  current primary view emphasizes batting and contact-quality metrics.
- Rare mid-plate-appearance batter or pitcher substitutions may require more
  specialized official-credit resolution than the terminal result participant.
- Player identity comes from retained game observations; the player dimension
  is not an authoritative current-roster endpoint.
- Public values reflect the latest successful acquisition, projection, dbt, and
  Hex publication boundaries rather than a live in-game feed.

## Suggested Hex methodology tab

The sections above can be represented in Hex as four compact blocks:

1. **How the data is built** — show the lineage diagram and link to the data
   platform.
2. **What MLB supplies vs. what Zavant calculates** — use the attribution lists.
3. **Metric definitions** — show the concise metric table and link to MetricFlow
   YAML.
4. **Quality and limitations** — show reconciliation checks, tracking coverage,
   data freshness, and current exclusions.

The public app should also display a maximum included game date so a viewer can
distinguish metric correctness from data freshness.
