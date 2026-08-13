with derived_at_bats as (
    select
        game_pk,
        source_revision_id,
        offense_team_id as team_id,
        count(*) as at_bats
    from {{ ref("int_at_bats") }}
    group by 1, 2, 3
),

official_at_bats as (
    select
        game_pk,
        source_revision_id,
        team_id,
        sum(at_bats) as at_bats
    from {{ ref("stg_boxscore_player_batting") }}
    group by 1, 2, 3
),

reconciled as (
    select
        coalesce(derived_counts.game_pk, official_boxscore.game_pk) as game_pk,
        coalesce(derived_counts.source_revision_id, official_boxscore.source_revision_id)
            as source_revision_id,
        coalesce(derived_counts.team_id, official_boxscore.team_id) as team_id,
        coalesce(derived_counts.at_bats, 0) as derived_at_bats,
        coalesce(official_boxscore.at_bats, 0) as official_at_bats
    from derived_at_bats as derived_counts
    full outer join official_at_bats as official_boxscore
        on
            derived_counts.game_pk = official_boxscore.game_pk
            and derived_counts.source_revision_id = official_boxscore.source_revision_id
            and derived_counts.team_id = official_boxscore.team_id
)

select *
from reconciled
where derived_at_bats != official_at_bats
