with player_positions as (
    select distinct
        game_pk,
        player_id,
        team_id
    from {{ ref("stg_boxscore_player_positions") }}
)

select
    a.game_pk,
    a.player_id,
    a.team_id,
    a.is_on_bench,
    b.games_played as batting_games_played,
    c.games_played as pitching_games_played,
    c.games_pitched,
    d.game_pk is not null as has_position
from {{ ref("stg_boxscore_players") }} as a
left join {{ ref("stg_boxscore_player_batting") }} as b
    on
        a.game_pk = b.game_pk
        and a.player_id = b.player_id
        and a.team_id = b.team_id
left join {{ ref("stg_boxscore_player_pitching") }} as c
    on
        a.game_pk = c.game_pk
        and a.player_id = c.player_id
        and a.team_id = c.team_id
left join player_positions as d
    on
        a.game_pk = d.game_pk
        and a.player_id = d.player_id
        and a.team_id = d.team_id
where
    a.is_on_bench
    and (
        coalesce(b.games_played, 0) > 0
        or coalesce(c.games_played, 0) > 0
        or coalesce(c.games_pitched, 0) > 0
        or d.game_pk is not null
    )
