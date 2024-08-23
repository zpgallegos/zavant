with src as (
    select 
        b.batter_id,
        a.play_id,
        a.event_idx,
        case when a.is_last_pitch = 1 then b.play_result else a.call_desc_recode end as pitch_result
    from {{ ref('int_ab_outcomes__pitches') }} a
        inner join {{ ref('int_ab_outcomes__plays') }} b on a.play_id = b.play_id
    where b.batter_id in(select player_id from {{ ref('included_players') }})
),
joined as (
    select
        a.batter_id,
        a.play_id,
        array_join(array_agg(a.pitch_result order by a.event_idx), '-') as seq
    from src a
    group by 1, 2
)

select
    a.batter_id,
    a.seq,
    count(distinct a.play_id) as cnt
from joined a
group by 1, 2
