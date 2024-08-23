with src as (
    select a.* 
    from {{ ref('standard_batting') }} a
        inner join {{ ref('included_players') }} b on a.player_id = b.player_id
)

select * from src