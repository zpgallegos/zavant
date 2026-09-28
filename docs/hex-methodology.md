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

Baseball Savant's separately acquired, revisioned CSV data also flows through
Glue into the combined batting facts. Savant supplies contact velocity/angle,
barrel classifications, and expected-outcome values; Stats API supplies the
event grain and official outcomes. The batted-ball fact retains original Stats
API velocity/angle alongside the Savant values so measurement coverage remains
auditable. Combined facts are rebuilt when either source revision changes.

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
- player-season membership from actual game participation and baseball age as
  of June 30 of the season year;
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
| xBA | Expected hits for official at-bats divided by strikeout at-bats plus contact at-bats with supplied hit probabilities. Missing contact estimates are excluded; zero estimates remain eligible. |
| xSLG | Expected total bases for official at-bats divided by strikeout at-bats plus contact at-bats with supplied total-base estimates. Coverage is evaluated independently of xBA. |
| xwOBA | Sum of Savant expected values weighted by `woba_denom`, divided by the sum of those denominator contributions. |
| wOBA | Official batting outcomes weighted with their season's FanGraphs coefficients, divided by AB + unintentional BB + HBP + SF. Current-season weights are provisional. |
| xwOBAcon | Sum of supplied expected wOBA values for batted balls divided by the number of non-null contact estimates. Includes home runs; excludes non-contact outcomes. |
| K% | Strikeouts divided by completed plate appearances. |
| BB% | Walks, including intentional walks, divided by completed plate appearances. |
| Batted-ball events | Count of projected batted-ball events. |
| Average exit velocity | Sum of Savant exit velocities, including supplied estimates, divided by events with a non-null Savant velocity. |
| Maximum exit velocity | Highest supplied Savant exit velocity in the selected population. |
| Average launch angle | Sum of Savant non-bunt launch angles divided by non-bunt events with a non-null Savant angle. Includes supplied estimates. |
| Barrel rate | Savant-classified barrels divided by events with both original Stats API contact measurements, an observed-contact proxy. |
| Hard-hit rate | Events with Savant exit velocity at least 95 mph divided by all batted-ball events. |
| Sweet-spot rate | Original Stats API angles from 8 through 32 degrees divided by events with an original Stats API angle. Intentionally unchanged pending reconciliation. |
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

MLB does not supply every tracking measurement for every batted ball.
[Savant's CSV documentation](https://baseballsavant.mlb.com/csv-docs) explains
that its velocity and angle fields include estimates for some untracked balls.
Eligibility is metric-specific:

- Contact averages use Savant observations, including supplied estimates;
  null values are excluded rather than averaged as zero. Average launch angle
  continues to exclude bunts.
- Hard-hit rate uses Savant velocity for the numerator and all BBE for the
  denominator. A missing velocity does not add a hard hit but does add a BBE.
- Barrel rate uses the original Stats API velocity-and-angle coverage as an
  observed-contact proxy. The extra Savant-filled values do not expand this
  denominator. This reconciles the checked Mookie season rates but is not an
  authoritative Savant eligibility flag, which our retained export lacks.
- Sweet-spot rate retains its original Stats API source and angle coverage;
  both its numerator and denominator are unchanged by the source correction.
- Statcast tracking rate reports Savant value availability, including supplied
  estimates, not the directly measured fraction.
- xBA/xSLG include strikeout at-bats as zero-value opportunities. Unestimated
  contact is excluded from the corresponding denominator; a supplied zero is
  included. No eligible opportunities produces a null rate.
- xwOBAcon requires a supplied expected wOBA contact value; missing estimates
  are excluded, while actual zero estimates count. It does not use all PAs or
  all BBE as its denominator when expected-contact coverage is incomplete.

The profile can therefore distinguish performance from measurement coverage.

Observed wOBA does not require Statcast tracking. Its annual coefficients live
in the versioned [wOBA reference seed](../dbt/seeds/README.md), with source and
retrieval dates. Each season's weights are applied before career aggregation;
rounded reference coefficients need not reproduce every Savant aggregate
exactly. Changes to those weights invalidate the affected season's PA facts.

## Remaining Savant reconciliation

The 2026-09-28 correction addresses confirmed source-coverage and denominator
differences, not a promise of exact agreement with every player-page aggregate.
Mookie's 2020 xBA/xSLG reconcile after excluding two unestimated contact at-bats;
small residual expected-stat differences remain in some older seasons. Observed
wOBA, xwOBA, and xwOBAcon were not changed by this correction.

Sweet-spot reconciliation is explicitly deferred. For the checked 2025 Mookie
population, both retained feeds contain the same 531 whole-degree angles.
Inclusive 8–32 gives 212 qualifying balls (39.9%), strict 8–32 gives 192 (36.2%),
and neither reproduces Savant's 37.7%. Higher-precision internal angles are a
possible explanation, not a verified one. Do not change boundaries merely to
approximate the displayed rate. Future work should establish Savant's exact
event eligibility/precision before changing this metric.

## Validation

The published metrics are supported by several independent checks:

- A contact-source test verifies Savant values, preserved Stats API values,
  coverage/quality indicators, and the unchanged sweet-spot classification.
- Offline regressions execute the actual fact SQL and semantic expressions to
  distinguish missing estimates, true zeros, strikeouts, and metric-specific
  denominators without modifying the warehouse.
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

- The profile does not yet calculate percentile rankings, swing decisions, or
  fielding value.
- Governed pitch and baserunning models exist, but the published profile's
  current primary view emphasizes batting and contact-quality metrics.
- Rare mid-plate-appearance batter or pitcher substitutions may require more
  specialized official-credit resolution than the terminal result participant.
- Player identity comes from retained game observations; the player dimension
  is not an authoritative current-roster endpoint.
- Public values reflect the latest successful acquisition, projection, dbt, and
  Hex publication boundaries rather than a live in-game feed.
- Statcast metrics are reconstructed from retained event data. Source coverage
  and public event fields can differ from Savant's precomputed player-page
  metrics, so these are not guaranteed replicas of the displayed aggregates.

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
