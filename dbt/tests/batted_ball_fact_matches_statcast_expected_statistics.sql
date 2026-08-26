select
    a.game_pk,
    a.at_bat_index,
    a.expected_batting_average,
    b.estimated_ba_using_speedangle,
    a.expected_slugging_percentage,
    b.estimated_slg_using_speedangle,
    a.expected_weighted_on_base_average,
    b.estimated_woba_using_speedangle
from {{ ref("fct_batted_balls") }} as a
inner join {{ ref("stg_statcast_batting_events") }} as b
    on
        a.game_pk = b.game_pk
        and a.at_bat_index + 1 = b.at_bat_number
where
    a.expected_batting_average is distinct from b.estimated_ba_using_speedangle
    or a.expected_slugging_percentage is distinct from b.estimated_slg_using_speedangle
    or a.expected_weighted_on_base_average is distinct from b.estimated_woba_using_speedangle
