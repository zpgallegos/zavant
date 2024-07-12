with src as (
    select 
        a.*,
        b.offense_team_id as team_id
    from {{ ref('stg_statsapi__runners') }} a
        inner join {{ ref('stg_statsapi__play_info') }} b on a.play_id = b.play_id
)

select * from src