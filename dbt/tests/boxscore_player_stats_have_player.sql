with batting_failures as (
    select
        'stg_boxscore_player_batting' as child_model,
        batting.game_pk,
        batting.player_id,
        batting.team_id
    from {{ ref("stg_boxscore_player_batting") }} as batting
    left join {{ ref("stg_boxscore_players") }} as player
        on
            batting.game_pk = player.game_pk
            and batting.player_id = player.player_id
            and batting.team_id = player.team_id
    where player.game_pk is null
),

pitching_failures as (
    select
        'stg_boxscore_player_pitching' as child_model,
        pitching.game_pk,
        pitching.player_id,
        pitching.team_id
    from {{ ref("stg_boxscore_player_pitching") }} as pitching
    left join {{ ref("stg_boxscore_players") }} as player
        on
            pitching.game_pk = player.game_pk
            and pitching.player_id = player.player_id
            and pitching.team_id = player.team_id
    where player.game_pk is null
),

fielding_failures as (
    select
        'stg_boxscore_player_fielding' as child_model,
        fielding.game_pk,
        fielding.player_id,
        fielding.team_id
    from {{ ref("stg_boxscore_player_fielding") }} as fielding
    left join {{ ref("stg_boxscore_players") }} as player
        on
            fielding.game_pk = player.game_pk
            and fielding.player_id = player.player_id
            and fielding.team_id = player.team_id
    where player.game_pk is null
)

select * from batting_failures
union all
select * from pitching_failures
union all
select * from fielding_failures
