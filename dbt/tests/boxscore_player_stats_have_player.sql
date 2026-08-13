with batting_failures as (
    select
        'stg_boxscore_player_batting' as child_model,
        batting.game_pk,
        batting.player_id,
        batting.team_id,
        batting.source_revision_id,
        batting.projection_contract_version
    from {{ ref("stg_boxscore_player_batting") }} as batting
    left join {{ ref("stg_boxscore_players") }} as player
        on
            batting.game_pk = player.game_pk
            and batting.player_id = player.player_id
            and batting.team_id = player.team_id
            and batting.source_revision_id = player.source_revision_id
            and batting.projection_contract_version = player.projection_contract_version
    where player.game_pk is null
),

pitching_failures as (
    select
        'stg_boxscore_player_pitching' as child_model,
        pitching.game_pk,
        pitching.player_id,
        pitching.team_id,
        pitching.source_revision_id,
        pitching.projection_contract_version
    from {{ ref("stg_boxscore_player_pitching") }} as pitching
    left join {{ ref("stg_boxscore_players") }} as player
        on
            pitching.game_pk = player.game_pk
            and pitching.player_id = player.player_id
            and pitching.team_id = player.team_id
            and pitching.source_revision_id = player.source_revision_id
            and pitching.projection_contract_version = player.projection_contract_version
    where player.game_pk is null
),

fielding_failures as (
    select
        'stg_boxscore_player_fielding' as child_model,
        fielding.game_pk,
        fielding.player_id,
        fielding.team_id,
        fielding.source_revision_id,
        fielding.projection_contract_version
    from {{ ref("stg_boxscore_player_fielding") }} as fielding
    left join {{ ref("stg_boxscore_players") }} as player
        on
            fielding.game_pk = player.game_pk
            and fielding.player_id = player.player_id
            and fielding.team_id = player.team_id
            and fielding.source_revision_id = player.source_revision_id
            and fielding.projection_contract_version = player.projection_contract_version
    where player.game_pk is null
)

select * from batting_failures
union all
select * from pitching_failures
union all
select * from fielding_failures
