with src as (
    select * from {{ source('statsapi', 'play_info') }}
)

select
    a.partition_0 as season,
    cast(a.game_pk as varchar) || '-' || cast(a.play_idx as varchar) as play_id,
    a.game_pk,
    a.play_idx,
    a.offense_team_id,
    a.defense_team_id,
    a.result_event as event,
    a.result_eventtype as event_code,
    a.result_isout as is_out,
    a.result_rbi as rbi,
    a.result_awayscore as away_score,
    a.result_homescore as home_score,
    a.about_inning as inning,
    a.about_halfinning as half_inning,
    a.about_hasout as has_out,
    a.about_iscomplete as is_complete,
    a.about_isscoringplay as is_scoring_play,
    a.count_balls,
    a.count_strikes,
    a.count_outs,
    a.matchup_batter_id as batter_id,
    a.matchup_batter_fullname as batter_fullname,
    a.matchup_batside_code as batside_code,
    a.matchup_splits_batter as batter_splits,
    a.matchup_pitcher_id as pitcher_id,
    a.matchup_pitcher_fullname as pitcher_fullname,
    a.matchup_pitchhand_code as pitchhand_code,
    a.matchup_splits_pitcher as pitcher_splits,
    a.matchup_splits_menonbase as men_on_base,
    a.matchup_postonfirst_id as postonfirst_id,
    a.matchup_postonsecond_id as postonsecond_id,
    a.matchup_postonthird_id as postonthird_id

from src a
