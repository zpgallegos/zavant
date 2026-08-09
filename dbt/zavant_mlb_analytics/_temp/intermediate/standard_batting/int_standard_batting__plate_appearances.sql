with src as (
    select * from {{ ref('int_standard_batting__play_info') }}
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
                play_result_code like 'pickoff%' or
                play_result_code like 'caught%' or
                play_result_code like 'other_out%'
                )
        ) and not (
            is_last_play = 1 and (
                play_result_code like '%wild_pitch%' or
                play_result_code like '%passed_ball%' or
                play_result_code like '%balk%'
                )
        )
),
tbl as (
    select
        a.batter_id as player_id,
        a.season,
        a.is_single_team_player as complete,
        a.offense_team_id as team_id,
        'plate_appearances' as stat,
        count(1) as value
    from filtered a
    group by 1, 2, 3, 4, 5
),
tot as (
    select
        a.batter_id as player_id,
        a.season,
        1 as complete,
        0 as team_id,
        'plate_appearances' as stat,
        count(1) as value
    from filtered a
    where a.is_single_team_player = 0
    group by 1, 2, 3, 4, 5
)

select * from tbl union
select * from tot
