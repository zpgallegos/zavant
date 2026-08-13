with fielding_credits as (
    select * from {{ source("zavant_analytical_prod", "fielding_credits") }}
)

select
    -- grain
    fielding_credits.game_pk,
    fielding_credits.at_bat_index,
    fielding_credits.runner_index,
    fielding_credits.credit_index,

    -- attributes
    fielding_credits.credit,
    fielding_credits.play_event_index,
    fielding_credits.player_id,
    fielding_credits.position_abbreviation,
    fielding_credits.position_code,
    fielding_credits.position_name,
    fielding_credits.position_type,

    -- metadata
    fielding_credits.official_date,
    fielding_credits.season
from fielding_credits
