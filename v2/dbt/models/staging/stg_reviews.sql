with current_revision as (
    select
        game_pk,
        source_revision_id,
        projection_contract_version
    from {{ ref("stg_current_game_revisions") }}
),

reviews as (
    select * from {{ source("zavant_analytical_prod", "reviews") }}
)

select
    -- grain
    reviews.game_pk,
    reviews.at_bat_index,
    reviews.review_id,

    -- attributes
    reviews.challenge_team_id,
    reviews.event_index,
    reviews.in_progress,
    reviews.is_overturned,
    reviews.player_id,
    reviews.review_scope,
    reviews.review_sequence,
    reviews.review_type,

    -- metadata
    reviews.official_date,
    reviews.projected_at,
    reviews.projection_contract_version,
    reviews.projection_run_id,
    reviews.season,
    reviews.source_revision_id
from reviews
inner join current_revision on
    reviews.game_pk = current_revision.game_pk
    and reviews.source_revision_id = current_revision.source_revision_id
    and reviews.projection_contract_version = current_revision.projection_contract_version
