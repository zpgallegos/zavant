with plays as (
    select * from {{ source("zavant_analytical_prod", "plays") }}
),

recovered_reviewed_outs as (
    -- Some historical final plays retain the review advisory as their result.
    -- Recover only one unambiguous batter-out movement; rain delays, unfinished
    -- games, other runners' outs, and conflicting movements remain untouched.
    select
        plays.game_pk,
        plays.at_bat_index,
        min(runners.event) as event,
        min(runners.event_type) as event_type
    from plays
    inner join {{ ref("stg_games") }} as games on plays.game_pk = games.game_pk
    inner join {{ ref("stg_runner_movements") }} as runners
        on
            plays.game_pk = runners.game_pk
            and plays.at_bat_index = runners.at_bat_index
            and plays.batter_id = runners.runner_id
    where
        plays.event_type = 'game_advisory'
        and plays.has_review = true
        and games.abstract_game_state = 'Final'
        and runners.origin_base is null
        and runners.start_base is null
    group by 1, 2
    having
        count(*) = 1
        and max(
            case
                when
                    runners.is_out = true
                    and runners.out_base = '1B'
                    and runners.event_type in ('field_out', 'grounded_into_double_play')
                    then 1
                else 0
            end
        ) = 1
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
    coalesce(recovered.event, plays.event) as event,
    coalesce(recovered.event_type, plays.event_type) as event_type,
    plays.half_inning,
    plays.has_out,
    plays.has_review,
    plays.home_score,
    plays.inning,
    case when recovered.game_pk is not null then true else plays.is_complete end as is_complete,
    case when recovered.game_pk is not null then true else plays.is_out end as is_out,
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

    -- Source values make the narrow historical normalization inspectable.
    plays.event as reported_event,
    plays.event_type as reported_event_type,
    plays.is_complete as reported_is_complete,
    plays.is_out as reported_is_out,
    recovered.game_pk is not null as is_batting_outcome_recovered,

    -- metadata
    plays.official_date,
    plays.season
from plays
left join recovered_reviewed_outs as recovered
    on
        plays.game_pk = recovered.game_pk
        and plays.at_bat_index = recovered.at_bat_index
