with fact_participations as (
    select
        game_pk,
        player_id,
        team_id
    from {{ ref("fct_player_game_participation") }}
),

official_participations as (
    select
        game_pk,
        player_id,
        team_id
    from {{ ref("stg_boxscore_player_batting") }}
    where games_played > 0

    union

    select
        game_pk,
        player_id,
        team_id
    from {{ ref("stg_boxscore_player_pitching") }}
    where games_played > 0 or games_pitched > 0
),

reconciled as (
    select
        coalesce(a.game_pk, b.game_pk) as game_pk,
        coalesce(a.player_id, b.player_id) as player_id,
        coalesce(a.team_id, b.team_id) as team_id,
        a.game_pk is not null as exists_in_fact,
        b.game_pk is not null as has_official_participation_evidence
    from fact_participations as a
    full outer join official_participations as b
        on
            a.game_pk = b.game_pk
            and a.player_id = b.player_id
            and a.team_id = b.team_id
)

select *
from reconciled
where exists_in_fact != has_official_participation_evidence
