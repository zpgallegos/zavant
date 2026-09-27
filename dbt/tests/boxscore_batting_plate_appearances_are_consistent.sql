-- Every AB, walk, HBP, sacrifice, and catcher-interference award consumes a PA.
-- Extra PA categories can exist, so this is a lower bound, not a universal
-- equality. Unknown shortfalls must fail rather than being silently normalized.
with batting_lines as (
    select
        'player' as line_type,
        game_pk,
        team_id,
        player_id,
        reported_plate_appearances,
        plate_appearances,
        plate_appearances_correction_reason,
        at_bats + base_on_balls + hit_by_pitch + sac_bunts + sac_flies
        + catchers_interference as minimum_plate_appearances
    from {{ ref("stg_boxscore_player_batting") }}

    union all

    select
        'team' as line_type,
        game_pk,
        team_id,
        cast(null as bigint) as player_id,
        reported_plate_appearances,
        plate_appearances,
        plate_appearances_correction_reason,
        at_bats + base_on_balls + hit_by_pitch + sac_bunts + sac_flies
        + catchers_interference as minimum_plate_appearances
    from {{ ref("stg_boxscore_team_batting") }}
)

select *
from batting_lines
where plate_appearances < minimum_plate_appearances
