with weights as (
    {{ annual_woba_weights() }}
)

select a.game_pk, a.at_bat_index
from {{ ref("fct_plate_appearances") }} as a
left join weights as b on a.season = b.season
where
    a.woba_weights_revision is distinct from b.woba_weights_revision
    or a.woba_weights_is_provisional is distinct from b.is_provisional
    or a.woba_opportunity_ind is distinct from (
        a.at_bat_ind + a.walk_ind - a.intentional_walk_ind + a.hit_by_pitch_ind + a.sac_fly_ind
    )
    or a.woba_numerator is null
    or abs(
        a.woba_numerator - (
            (a.walk_ind - a.intentional_walk_ind) * b.walk_weight
            + a.hit_by_pitch_ind * b.hit_by_pitch_weight
            + a.single_ind * b.single_weight
            + a.double_ind * b.double_weight
            + a.triple_ind * b.triple_weight
            + a.home_run_ind * b.home_run_weight
        )
    ) > 0.000000001
