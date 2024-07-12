/*
create a table that defines landmarks of the strikezone for each batter. used in pitching
analyses that require zone-based metrics. this table can be joined with the pitches and 
using together with the coord_px/coord_pz (horizontal/vertical coordinates of the plate
crossing of the pitch) to determine which zone the pitch belongs to. this version uses a 
3x3 grid inside the zone (configurable), as well as four perimeter zones that are the 
width of a baseball outside the strike zone ("one ball off the plate").
*/

with grid as (
    select * from {{ ref('utils_zone3grid') }} -- configure
),
dims as (
    select
        max(zone_row) + 1 as nrows,
        max(zone_col) + 1 as ncols,
        max(zone) as nzones
    from grid
),
src as (
    select
        player_id as batter_id,
        batside_code,
        strikezone_top,
        strikezone_bottom,
        0.09754885882352941 as statcast_inch -- 1 inch in Statcast units
    from {{ ref('stg_statsapi__player_info') }}
), 
trans1 as (
    select
        a.*,
        statcast_inch * 8.5 as half_plate, -- plate is 17 inches wide
        statcast_inch * 2.9 as ball -- baseball is 2.9 inches in diameter
    from src a
),
trans2 as (
    select
        a.*,
        -a.half_plate as strikezone_left,
        a.half_plate as strikezone_right,
        a.half_plate * 2 as strikezone_width,
        a.strikezone_top - a.strikezone_bottom as strikezone_height
    from trans1 a
),
trans3 as (
    select
        a.*,
        a.strikezone_width / (select nrows from dims) as width_each,
        a.strikezone_height / (select ncols from dims) as height_each,
        a.strikezone_top + a.ball as perimeter_top,
        a.strikezone_bottom - a.ball as perimeter_bottom,
        a.strikezone_left - a.ball as perimeter_left,
        a.strikezone_right + a.ball as perimeter_right,
        0 as mid_width,
        a.strikezone_bottom + a.strikezone_height / 2 as mid_height
    from trans2 a
),
in_zone as (
    -- zones inside the strike zone

    select 
        a.*,
        b.zone,
        'zone' as zone_type,
        a.strikezone_left + a.width_each * b.zone_col as zone_left,
        a.strikezone_top - a.height_each * b.zone_row as zone_top,
        a.strikezone_left + a.width_each * (b.zone_col + 1) as zone_right,
        a.strikezone_top - a.height_each * (b.zone_row + 1) as zone_bottom
    from trans3 a 
        cross join grid b
),
perimeter_zone as (
    -- zones in the ball-sized immediate perimeter around the strike zone
    -- have real-world importance because this is where pitchers try to throw

    select
        a.*,
        (select nzones + 1 from dims) as zone,
        'perimeter' as zone_type,
        a.strikezone_left as zone_left,
        a.perimeter_top as zone_top,
        a.strikezone_right as zone_right,
        a.strikezone_top as zone_bottom
    from trans3 a

    union

    select
        a.*,
        (select nzones + 2 from dims) as zone,
        'perimeter' as zone_type,
        a.strikezone_right as zone_left,
        a.perimeter_top as zone_top,
        a.perimeter_right as zone_right,
        a.perimeter_bottom as zone_bottom
    from trans3 a

    union

    select
        a.*,
        (select nzones + 3 from dims) as zone,
        'perimeter' as zone_type,
        a.strikezone_left as zone_left,
        a.strikezone_bottom as zone_top,
        a.strikezone_right as zone_right,
        a.perimeter_bottom as zone_bottom
    from trans3 a

    union

    select
        a.*,
        (select nzones + 4 from dims) as zone,
        'perimeter' as zone_type,
        a.perimeter_left as zone_left,
        a.perimeter_top as zone_top,
        a.strikezone_left as zone_right,
        a.perimeter_bottom as zone_bottom
    from trans3 a
),
outside as (
    -- catch-all areas for balls outside the strike zone

    select
        a.*,
        (select nzones + 5 from dims) as zone,
        'outside' as zone_type,
        a.perimeter_left as zone_left,
        999 as zone_top,
        a.perimeter_right as zone_right,
        a.perimeter_top as zone_bottom
    from trans3 a

    union
    select
        a.*,
        (select nzones + 6 from dims) as zone,
        'outside' as zone_type,
        a.perimeter_right as zone_left,
        999 as zone_top,
        999 as zone_right,
        -999 as zone_bottom
    from trans3 a

    union

    select
        a.*,
        (select nzones + 7 from dims) as zone,
        'outside' as zone_type,
        a.perimeter_left as zone_left,
        a.perimeter_bottom as zone_top,
        a.perimeter_right as zone_right,
        -999 as zone_bottom
    from trans3 a

    union

    select
        a.*,
        (select nzones + 8 from dims) as zone,
        'outside' as zone_type,
        -999 as zone_left,
        999 as zone_top,
        a.perimeter_left as zone_right,
        -999 as zone_bottom
    from trans3 a
),
cte as (
    select * from in_zone union
    select * from perimeter_zone  union
    select * from outside
)

select
    batter_id,
    zone,
    zone_type,
    zone_left,
    zone_top,
    zone_right,
    zone_bottom
from cte
