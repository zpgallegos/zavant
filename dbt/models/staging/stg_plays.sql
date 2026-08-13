with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

plays as (
    select * from {{ source("zavant_analytical_prod", "plays") }}
)

select
    -- grain
    plays.game_pk,
    plays.at_bat_index,

    -- attributes
    plays.away_score,
    plays.balls,
    plays.bat_side_code,
    plays.batter_id,
    plays.batter_split,
    plays.captivating_index,
    plays.defense_team_id,
    plays.description,
    plays.ended_at,
    plays.event,
    plays.event_type,
    plays.half_inning,
    plays.has_out,
    plays.has_review,
    plays.home_score,
    plays.inning,
    plays.is_complete,
    plays.is_out,
    plays.is_scoring_play,
    plays.is_top_inning,
    plays.men_on_base_split,
    plays.offense_team_id,
    plays.outs,
    plays.pitch_hand_code,
    plays.pitcher_id,
    plays.pitcher_split,
    plays.play_type,
    plays.post_on_first_id,
    plays.post_on_second_id,
    plays.post_on_third_id,
    plays.rbi,
    plays.started_at,
    plays.strikes,

    -- metadata
    plays.official_date,
    plays.projected_at,
    plays.projection_contract_version,
    plays.projection_run_id,
    plays.season,
    plays.source_revision_id
from plays
inner join current_revision on
    plays.game_pk = current_revision.game_pk
    and plays.source_revision_id = current_revision.source_revision_id
    and plays.projection_contract_version = current_revision.projection_contract_version
