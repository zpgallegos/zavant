with derived_statistics as (
    select
        game_pk,
        offense_team_id as team_id,
        sum(plate_appearance_count) as plate_appearances,
        sum(at_bat_count) as at_bats,
        sum(hit_count) as hits,
        sum(total_bases) as total_bases,
        sum(walk_count) as base_on_balls,
        sum(intentional_walk_count) as intentional_walks,
        sum(hit_by_pitch_count) as hit_by_pitch,
        sum(strikeout_count) as strike_outs,
        sum(sac_bunt_count) as sac_bunts,
        sum(sac_fly_count) as sac_flies,
        sum(coalesce(rbi, 0)) as rbi
    from {{ ref("fct_plate_appearances") }}
    group by 1, 2
),

official_statistics as (
    select
        player_batting.game_pk,
        player_batting.team_id,
        sum(player_batting.plate_appearances) as plate_appearances,
        sum(player_batting.at_bats) as at_bats,
        sum(player_batting.hits) as hits,
        sum(player_batting.total_bases) as total_bases,
        sum(player_batting.base_on_balls) as base_on_balls,
        sum(player_batting.intentional_walks) as intentional_walks,
        sum(player_batting.hit_by_pitch) as hit_by_pitch,
        sum(player_batting.strike_outs) as strike_outs,
        sum(player_batting.sac_bunts) as sac_bunts,
        sum(player_batting.sac_flies) as sac_flies,
        sum(player_batting.rbi) as rbi
    from {{ ref("stg_boxscore_player_batting") }} as player_batting
    group by 1, 2
),

reconciled as (
    select
        coalesce(derived_counts.game_pk, official_counts.game_pk) as game_pk,
        coalesce(derived_counts.team_id, official_counts.team_id) as team_id,
        coalesce(derived_counts.plate_appearances, 0) as derived_plate_appearances,
        coalesce(official_counts.plate_appearances, 0) as official_plate_appearances,
        coalesce(derived_counts.at_bats, 0) as derived_at_bats,
        coalesce(official_counts.at_bats, 0) as official_at_bats,
        coalesce(derived_counts.hits, 0) as derived_hits,
        coalesce(official_counts.hits, 0) as official_hits,
        coalesce(derived_counts.total_bases, 0) as derived_total_bases,
        coalesce(official_counts.total_bases, 0) as official_total_bases,
        coalesce(derived_counts.base_on_balls, 0) as derived_base_on_balls,
        coalesce(official_counts.base_on_balls, 0) as official_base_on_balls,
        coalesce(derived_counts.intentional_walks, 0) as derived_intentional_walks,
        coalesce(official_counts.intentional_walks, 0) as official_intentional_walks,
        coalesce(derived_counts.hit_by_pitch, 0) as derived_hit_by_pitch,
        coalesce(official_counts.hit_by_pitch, 0) as official_hit_by_pitch,
        coalesce(derived_counts.strike_outs, 0) as derived_strike_outs,
        coalesce(official_counts.strike_outs, 0) as official_strike_outs,
        coalesce(derived_counts.sac_bunts, 0) as derived_sac_bunts,
        coalesce(official_counts.sac_bunts, 0) as official_sac_bunts,
        coalesce(derived_counts.sac_flies, 0) as derived_sac_flies,
        coalesce(official_counts.sac_flies, 0) as official_sac_flies,
        coalesce(derived_counts.rbi, 0) as derived_rbi,
        coalesce(official_counts.rbi, 0) as official_rbi
    from derived_statistics as derived_counts
    full outer join official_statistics as official_counts
        on
            derived_counts.game_pk = official_counts.game_pk
            and derived_counts.team_id = official_counts.team_id
)

select *
from reconciled
where
    derived_plate_appearances != official_plate_appearances
    or derived_at_bats != official_at_bats
    or derived_hits != official_hits
    or derived_total_bases != official_total_bases
    or derived_base_on_balls != official_base_on_balls
    or derived_intentional_walks != official_intentional_walks
    or derived_hit_by_pitch != official_hit_by_pitch
    or derived_strike_outs != official_strike_outs
    or derived_sac_bunts != official_sac_bunts
    or derived_sac_flies != official_sac_flies
    or derived_rbi != official_rbi
