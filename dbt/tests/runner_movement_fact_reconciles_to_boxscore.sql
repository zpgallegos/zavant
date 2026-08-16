with derived_statistics as (
    select
        game_pk,
        runner_id as player_id,
        offense_team_id as team_id,
        sum(run_scored_ind) as runs,
        sum(stolen_base_ind) as stolen_bases,
        sum(caught_stealing_ind) as caught_stealing
    from {{ ref("fct_runner_movements") }}
    group by 1, 2, 3
),

official_statistics as (
    select
        game_pk,
        player_id,
        team_id,
        sum(runs) as runs,
        sum(stolen_bases) as stolen_bases,
        sum(caught_stealing) as caught_stealing
    from {{ ref("stg_boxscore_player_batting") }}
    group by 1, 2, 3
),

reconciled as (
    select
        coalesce(a.game_pk, b.game_pk) as game_pk,
        coalesce(a.player_id, b.player_id) as player_id,
        coalesce(a.team_id, b.team_id) as team_id,
        coalesce(a.runs, 0) as derived_runs,
        coalesce(b.runs, 0) as official_runs,
        coalesce(a.stolen_bases, 0) as derived_stolen_bases,
        coalesce(b.stolen_bases, 0) as official_stolen_bases,
        coalesce(a.caught_stealing, 0) as derived_caught_stealing,
        coalesce(b.caught_stealing, 0) as official_caught_stealing
    from derived_statistics as a
    full outer join official_statistics as b
        on
            a.game_pk = b.game_pk
            and a.player_id = b.player_id
            and a.team_id = b.team_id
)

select *
from reconciled
where
    derived_runs != official_runs
    or derived_stolen_bases != official_stolen_bases
    or derived_caught_stealing != official_caught_stealing
