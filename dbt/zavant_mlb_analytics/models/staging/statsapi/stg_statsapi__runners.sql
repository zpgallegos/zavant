with src as (
    select * from {{ source('statsapi', 'play_runners') }}
)

select
    a.partition_0 as season,
    cast(a.game_pk as varchar) || '-' || cast(a.play_idx as varchar) as play_id,
    cast(a.game_pk as varchar) || '-' || cast(a.play_idx as varchar) || '-' || cast(a.runner_idx as varchar) as movement_id,
    a.details_runner_id as runner_id,
    a.details_runner_fullname as runner_fullname,
    a.game_pk,
    a.play_idx,
    a.runner_idx,
    a.movement_start,
    a.movement_end,
    a.movement_isout as is_out,
    a.movement_outnumber as out_number,
    a.movement_outbase as out_base,
    a.details_event as run_event,
    a.details_eventtype as run_event_code,
    a.details_movementreason as movement_reason,
    a.details_rbi as is_rbi,
    a.details_isscoringevent as is_scoring_event,
    a.details_earned as is_earned,
    a.details_teamunearned as is_team_unearned,
    a.details_responsiblepitcher_id as responsible_pitcher_id

from src a