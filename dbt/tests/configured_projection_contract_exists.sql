select
    '{{ var("current_projection_contract_version") }}' as projection_contract_version
where not exists (
    select 1 as row_exists
    from {{ ref('stg_current_game_revisions') }}
)
