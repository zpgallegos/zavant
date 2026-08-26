{% macro changed_statsapi_game_revisions() %}
    select
        a.game_pk,
        a.statsapi_source_revision_id
    from {{ ref("stg_games") }} as a
    {% if is_incremental() %}
        where not exists (
            select 1 as row_exists
            from {{ this }} as b
            where
                a.game_pk = b.game_pk
                and a.statsapi_source_revision_id = b.statsapi_source_revision_id
        )
    {% endif %}
{% endmacro %}


{% macro changed_statsapi_and_savant_game_revisions() %}
    select
        a.game_pk,
        a.statsapi_source_revision_id,
        b.savant_source_revision_id
    from {{ ref("stg_games") }} as a
    left join {{ ref("stg_statcast_date_revisions") }} as b
        on a.official_date = b.game_date
    {% if is_incremental() %}
        where not exists (
            select 1 as row_exists
            from {{ this }} as c
            where
                a.game_pk = c.game_pk
                and a.statsapi_source_revision_id = c.statsapi_source_revision_id
                and b.savant_source_revision_id is not distinct from c.savant_source_revision_id
        )
    {% endif %}
{% endmacro %}


{% macro correction_safe_merge_rows(final_cte, changed_games_cte, unique_key) %}
{% if is_incremental() %}
        -- Desired rows become the merge update and insert set.
        select
            a.*,
            false as _dbt_is_deleted
        from {{ final_cte }} as a

        union all

        -- Existing rows absent from the corrected game state become tombstones.
        select
            b.*,
            true as _dbt_is_deleted
        from {{ changed_games_cte }} as a
        inner join {{ this }} as b on a.game_pk = b.game_pk
        left join {{ final_cte }} as c
            on
                {% for column in unique_key %}
{% if not loop.first %}and {% endif %}b.{{ column }} = c.{{ column }}
                {% endfor %}
        where c.game_pk is null
    {% else %}
        select * from {{ final_cte }}
{% endif %}
{% endmacro %}
