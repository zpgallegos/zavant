with substitutions as (
    select * from {{ source("zavant_analytical_prod", "substitutions") }}
)

select
    -- grain
    substitutions.game_pk,
    substitutions.at_bat_index,
    substitutions.event_index,

    -- attributes
    substitutions.base_number,
    substitutions.batting_order,
    substitutions.description,
    substitutions.incoming_player_id,
    substitutions.play_id,
    substitutions.position_abbreviation,
    substitutions.position_code,
    substitutions.position_name,
    substitutions.replaced_player_id,
    substitutions.substitution_type,

    -- metadata
    substitutions.official_date,
    substitutions.season
from substitutions
