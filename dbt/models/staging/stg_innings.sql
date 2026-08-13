with innings as (
    select * from {{ source("zavant_analytical_prod", "innings") }}
)

select
    -- grain
    innings.source_revision_id,
    innings.game_pk,
    innings.inning_number,

    -- attributes
    innings.away_errors,
    innings.away_hits,
    innings.away_left_on_base,
    innings.away_runs,
    innings.home_errors,
    innings.home_hits,
    innings.home_left_on_base,
    innings.home_runs,
    innings.ordinal,

    -- metadata
    innings.official_date,
    innings.projected_at,
    innings.projection_run_id,
    innings.season
from innings
