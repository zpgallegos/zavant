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

    from {{ ref('stg_statsapi__pitches') }} a 
        inner join {{ ref('stg_statsapi__play_info') }} b  on a.play_id = b.play_id
        inner join {{ ref('stg_statsapi__player_info') }} c on b.batter_id = c.player_id

),
zoned as (

    select 
        a.*,
        b.zone,
        b.zone_type
    
    from cte a
        inner join {{ ref('stg_statsapi__strikezones') }} b
            on a.batter_id = b.batter_id
    
    where
        a.coord_px >= b.zone_left and
        a.coord_px < b.zone_right and
        a.coord_pz <= b.zone_top and
        a.coord_pz > b.zone_bottom

), missing as (

    select
        a.*,
        null as zone,
        null as zone_type
    
    from cte a
        left join zoned b on a.pitch_id = b.pitch_id
    
    where
        b.game_pk is null

)

select * from zoned
union all
select * from missing