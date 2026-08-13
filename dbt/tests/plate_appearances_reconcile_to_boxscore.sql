with derived_plate_appearances as (
    select
        game_pk,
        source_revision_id,
        offense_team_id as team_id,
        count(*) as plate_appearances
    from {{ ref("int_plate_appearances") }}
    group by 1, 2, 3
),

official_plate_appearances as (
    select
        game_pk,
        source_revision_id,
        team_id,
        sum(plate_appearances) as plate_appearances
    from {{ ref("stg_boxscore_player_batting") }}
    group by 1, 2, 3
),

reconciled as (
    select
        coalesce(derived_counts.game_pk, official_boxscore.game_pk) as game_pk,
        coalesce(derived_counts.source_revision_id, official_boxscore.source_revision_id)
            as source_revision_id,
        coalesce(derived_counts.team_id, official_boxscore.team_id) as team_id,
        coalesce(derived_counts.plate_appearances, 0) as derived_plate_appearances,
        coalesce(official_boxscore.plate_appearances, 0) as official_plate_appearances
    from derived_plate_appearances as derived_counts
    full outer join official_plate_appearances as official_boxscore
        on
            derived_counts.game_pk = official_boxscore.game_pk
            and derived_counts.source_revision_id = official_boxscore.source_revision_id
            and derived_counts.team_id = official_boxscore.team_id
)

select *
from reconciled
where derived_plate_appearances != official_plate_appearances
