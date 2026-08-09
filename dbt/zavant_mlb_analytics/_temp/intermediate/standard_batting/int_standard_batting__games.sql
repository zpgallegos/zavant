with src as (
    select a.* 
    from {{ ref('int_standard_batting__box_games') }} a
    where not a.is_on_bench
), tbl as (
    select
        a.player_id,
        a.season,
        a.is_single_team_player as complete,
        a.team_id,
        'games_played' as stat,
        count(distinct a.game_pk) as value
    from src a
    group by 1, 2, 3, 4, 5
), tot as (
    select
        a.player_id,
        a.season,
        1 as complete,
        0 as team_id,
        'games_played' as stat,
        count(distinct a.game_pk) as value
    from src a
    where a.is_single_team_player = 0
    group by 1, 2, 3, 4, 5
)

select * from tbl union
select * from tot
