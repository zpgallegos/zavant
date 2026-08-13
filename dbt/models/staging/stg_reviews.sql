with reviews as (
    select * from {{ source("zavant_analytical_prod", "reviews") }}
)

select
    -- grain
    reviews.game_pk,
    reviews.at_bat_index,
    reviews.review_id,

    -- attributes
    reviews.challenge_team_id,
    reviews.event_index,
    reviews.in_progress,
    reviews.is_overturned,
    reviews.player_id,
    reviews.review_scope,
    reviews.review_sequence,
    reviews.review_type,

    -- metadata
    reviews.official_date,
    reviews.season
from reviews
