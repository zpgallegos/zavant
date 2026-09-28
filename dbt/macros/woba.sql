{% macro annual_woba_weights() %}
    -- Hash numerical inputs and provisional status, not the retrieval timestamp.
    -- Editing a coefficient must invalidate already-materialized game rows.
    select
        *,
        {{ dbt_utils.generate_surrogate_key([
            "season", "walk_weight", "hit_by_pitch_weight", "single_weight",
            "double_weight", "triple_weight", "home_run_weight", "is_provisional"
        ]) }} as woba_weights_revision
    from {{ ref("woba_weights") }}
{% endmacro %}


{% macro validate_woba_weights() %}
    {# Runtime pre-hook: fail before the fact is written, even for a subset build.
       Seed tests alone cannot protect a build that does not select the seed. #}
    {% if execute %}
        {% set validation_sql %}
            select a.season
            from (select distinct season from {{ ref("stg_games") }}) as a
            left join {{ ref("woba_weights") }} as b on a.season = b.season
            group by a.season
            having
                count(b.season) != 1
                or sum(case
                    when b.walk_weight > 0 and b.walk_weight < 3
                        and b.hit_by_pitch_weight > 0 and b.hit_by_pitch_weight < 3
                        and b.single_weight > 0 and b.single_weight < b.double_weight
                        and b.double_weight < b.triple_weight
                        and b.triple_weight < b.home_run_weight and b.home_run_weight < 3
                        and b.is_provisional is not null
                    then 0 else 1
                end) > 0
            order by a.season
        {% endset %}
        {% set result = run_query(validation_sql) %}
        {% if result.rows | length > 0 %}
            {% set seasons = result.rows | map(attribute=0) | join(", ") %}
            {{ exceptions.raise_compiler_error(
                "Missing, duplicate, or invalid wOBA weights for seasons: " ~ seasons
                ~ ". Correct and load the woba_weights seed before building fct_plate_appearances."
            ) }}
        {% endif %}
    {% endif %}
{% endmacro %}
