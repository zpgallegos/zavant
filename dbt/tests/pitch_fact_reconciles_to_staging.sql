with fact_pitches as (
    select
        game_pk,
        at_bat_index,
        event_index
    from {{ ref("fct_pitches") }}
),

staged_pitches as (
    select
        game_pk,
        at_bat_index,
        event_index
    from {{ ref("stg_pitches") }}
),

reconciled as (
    select
        coalesce(a.game_pk, b.game_pk) as game_pk,
        coalesce(a.at_bat_index, b.at_bat_index) as at_bat_index,
        coalesce(a.event_index, b.event_index) as event_index,
        a.game_pk is not null as exists_in_fact,
        b.game_pk is not null as exists_in_staging
    from fact_pitches as a
    full outer join staged_pitches as b
        on
            a.game_pk = b.game_pk
            and a.at_bat_index = b.at_bat_index
            and a.event_index = b.event_index
)

select *
from reconciled
where exists_in_fact != exists_in_staging
