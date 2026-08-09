{{
    config(
        partitioned_by=["season", "batter_id"]
    )
}}

with cte as (
    select
        a.*,
        b.batter_id,
        b.batter_fullname,
        b.batside_code,
        b.batter_splits,
        c.strikezone_top,
        c.strikezone_bottom,
        b.pitcher_id,
        b.pitcher_fullname,
        b.pitchhand_code,
        b.pitcher_splits
    from {{ ref('stg_statsapi__pitches') }} as a
    inner join
        {{ ref('stg_statsapi__play_info') }} as b
        on a.play_id = b.play_id
    inner join
        {{ ref('stg_statsapi__player_info') }} as c
        on b.batter_id = c.player_id

),

zoned as (
    select
        a.*,
        b.zone,
        b.zone_type
    from cte as a
    inner join {{ ref('stg_statsapi__strikezones') }} as b
        on a.batter_id = b.batter_id
    where
        a.coord_px >= b.zone_left
        and a.coord_px < b.zone_right
        and a.coord_pz <= b.zone_top
        and a.coord_pz > b.zone_bottom

),

missing as (
    select
        a.*,
        null as zone,
        null as zone_type
    from cte as a
    left join zoned as b on a.pitch_id = b.pitch_id
    where
        b.game_pk is null
),

res as (
    select * from zoned
    union all
    select * from missing
)

select
    batter_fullname,
    batside_code,
    batter_splits,
    strikezone_top,
    strikezone_bottom,
    pitcher_id,
    pitcher_fullname,
    pitchhand_code,
    pitcher_splits,
    play_id,
    pitch_id,
    game_pk,
    play_idx,
    event_idx,
    pitch_number,
    pitch_type_code,
    pitch_type_desc,
    call_code,
    call_desc,
    count_balls,
    count_strikes,
    count_outs,
    is_ball,
    is_strike,
    is_in_play,
    is_out,
    break_angle,
    break_horizontal,
    break_vertical,
    break_length,
    break_vertical_induced,
    break_y,
    spin_direction,
    spin_rate,
    coord_ax,
    coord_ay,
    coord_az,
    coord_px,
    coord_pz,
    coord_pfxx,
    coord_pfxz,
    coord_vx0,
    coord_vy0,
    coord_vz0,
    coord_x,
    coord_x0,
    coord_y,
    coord_y0,
    coord_z0,
    start_speed,
    end_speed,
    extension,
    plate_time,
    hit_hardness,
    hit_launchspeed,
    hit_plate_location,
    hit_trajectory,
    season,
    batter_id
from res
