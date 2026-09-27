{% macro batter_first_base_awards() %}
    -- A credit belongs to a particular runner movement, not every batter outcome
    -- on that play. Advancing an existing runner (or the batter beyond first)
    -- does not turn an ordinary reached-on-error at-bat into a first-base award.
    select
        plays.game_pk,
        plays.at_bat_index,
        count(*) filter (where credits.credit = 'f_interference') > 0 as has_interference,
        count(*) filter (
            where credits.credit = 'f_defensive_shift_violation_error'
        ) > 0 as has_defensive_shift_violation
    from {{ ref("stg_plays") }} as plays
    inner join {{ ref("stg_runner_movements") }} as runners
        on
            plays.game_pk = runners.game_pk
            and plays.at_bat_index = runners.at_bat_index
            and plays.batter_id = runners.runner_id
    inner join {{ ref("stg_fielding_credits") }} as credits
        on
            runners.game_pk = credits.game_pk
            and runners.at_bat_index = credits.at_bat_index
            and runners.runner_index = credits.runner_index
    where
        plays.event_type = 'field_error'
        and runners.origin_base is null
        and runners.start_base is null
        and runners.end_base = '1B'
        and runners.is_out = false
        and credits.credit in ('f_interference', 'f_defensive_shift_violation_error')
    group by 1, 2
{% endmacro %}


{% macro boxscore_batting_with_pa_corrections(source_name, player_level) %}
    -- Preserve source statistics. Only repair a PA shortfall exactly explained
    -- by recovered reviewed outs AND the boxscore's independent AB/non-AB totals.
    -- Intentional walks are already included in base_on_balls; do not add twice.
    with reported as (
        select * from {{ source("zavant_analytical_prod", source_name) }}
    ),

    recovered as (
        select
            game_pk,
            offense_team_id as team_id,
            {% if player_level %}batter_id as player_id,{% endif %}
            count(*) as recovered_plate_appearances
        from {{ ref("stg_plays") }}
        where is_batting_outcome_recovered
        group by game_pk, offense_team_id{% if player_level %}, batter_id{% endif %}
    )

    select
        reported.*,
        case
            when
                recovered.recovered_plate_appearances > 0
                and reported.plate_appearances + recovered.recovered_plate_appearances
                = reported.at_bats + reported.base_on_balls + reported.hit_by_pitch
                + reported.sac_bunts + reported.sac_flies + reported.catchers_interference
                then reported.plate_appearances + recovered.recovered_plate_appearances
            else reported.plate_appearances
        end as corrected_plate_appearances
    from reported
    left join recovered
        on
            reported.game_pk = recovered.game_pk
            and reported.team_id = recovered.team_id
            {% if player_level %}and reported.player_id = recovered.player_id{% endif %}
{% endmacro %}
