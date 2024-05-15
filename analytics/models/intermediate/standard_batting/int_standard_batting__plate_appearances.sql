with source as (
    select
        *,
        case
        when row_number() over(partition by game_pk order by play_idx desc) = 1 then 1
        else 0
        end as is_last_play
    from zavant.play_info
), filtered as (
    select *
    from source
    where
        about_iscomplete = true and
        not (count_outs = 3 and (
            result_eventtype like 'pickoff%' or
            result_eventtype like 'caught%' or
            result_eventtype like 'other_out%'
            )
        ) and
        not (is_last_play = 1 and (
            result_eventtype like '%wild_pitch%' or
            result_eventtype like '%passed_ball%' or
            result_eventtype like '%balk%'
            )
        )
)

select
    partition_0,
    matchup_batter_id as player_id,
    offense_team_id as team_id,
    'plate_appearances' as stat,
    count(1) as value
from filtered
group by 1, 2, 3, 4
