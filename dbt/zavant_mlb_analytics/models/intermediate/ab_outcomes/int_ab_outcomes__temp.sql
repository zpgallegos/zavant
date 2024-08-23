with src as (
    select 
        a.*, 
        b.batter_id,
        b.play_result
    from {{ ref('int_ab_outcomes__pitches') }} a
        inner join {{ ref('int_ab_outcomes__plays') }} b on a.play_id = b.play_id
    where b.batter_id = 521692
)

select
    a.game_pk,
    a.play_idx,
    a.event_idx,
    a.play_id,
    a.pitch_id,
    a.pitch_number,
    a.is_last_pitch,
    a.end_count,
    a.count_changed,
    a.call_desc,
    a.call_desc_recode,
    case when a.is_last_pitch = 1 then a.play_result else a.call_desc_recode end as pitch_result,
    a.play_result,
    a.count_outs,
    a.is_in_play,
    a.is_out,
    a.hit_trajectory
from src a;
