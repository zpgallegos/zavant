with src as (
    select * from {{ ref('stg_statsapi__play_info') }}
),
with_last_play as (
    select
        a.*,
        case
        when row_number() over(partition by a.game_pk order by a.play_idx desc) = 1 then 1
        else 0
        end as is_last_play
    from src a
), filtered as (
    select a.*
    from with_last_play a
    where
        1=1
        and is_complete 
        and not (
            count_outs = 3 and (
                event_code like 'pickoff%' or
                event_code like 'caught%' or
                event_code like 'other_out%'
                )
        ) and not (
            is_last_play = 1 and (
                event_code like '%wild_pitch%' or
                event_code like '%passed_ball%' or
                event_code like '%balk%'
                )
        )
)

select
    season,
    batter_id as player_id,
    offense_team_id as team_id,
    'plate_appearances' as stat,
    count(1) as value
from filtered
group by 1, 2, 3, 4
