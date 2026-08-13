with source as (
    select
    -- grain
        game_pk,

        -- metadata
        projection_run_id,
        raw_object_uri,
        reconciled_at,
        season,
        source_revision_id
    from {{ source("zavant_analytical_prod", "current_game_revisions") }}
)

select * from source
