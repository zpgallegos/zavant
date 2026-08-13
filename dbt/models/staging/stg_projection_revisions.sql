with source as (
    select
    -- grain
        game_pk,
        source_revision_id,
        projection_contract_version,

        -- metadata
        projected_at,
        projection_run_id,
        raw_object_uri,
        season
    from {{ source("zavant_analytical_prod", "projection_revisions") }}
    where projection_contract_version = '{{ var("current_projection_contract_version") }}'
)

select * from source
