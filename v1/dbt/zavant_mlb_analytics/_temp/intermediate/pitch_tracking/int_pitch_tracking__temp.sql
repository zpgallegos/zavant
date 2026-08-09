with src as (
    select * from {{ ref('statcast_pitches') }}
)

select
    a.*,

    -- broadly categorize the pitches. according to baseball savant, they use these:
    -- Fastball: 4 Seam, 2 Seam, Cutter, Sinker 
    -- Offspeed: Split, Change, Fork, Screw
    -- Breaking: Slider, Curve, Knuckle, Sweeper, Slurve, Other

    case
    when a.pitch_type_code in(
        'FF', -- Four-seam Fastball
        'FT', -- Two-seam Fastball
        'FA', -- Fastball
        'FC', -- Cutter
        'SI'  -- Sinker
    ) then 'Fastball'
    when a.pitch_type_code in (
        'FS', -- Splitter
        'CH', -- Changeup
        'FO', -- Forkball
        'SC', -- Screwball
        'EP'  -- Eephus
    ) then 'Offspeed'
    when a.pitch_type_code in (
        'SL', -- Slider
        'CU', -- Curveball
        'KC', -- Knuckle Curve
        'KN', -- Knuckle Ball
        'ST', -- Sweeper
        'SV', -- Slurve
        'CS'  -- Slow Curve
    ) then 'Breaking'
    else 'Other'
    end as pitch_category


from src a
where 
    1=1
    -- and a.pitch_type_code is not null
    and a.pitch_type_code not in('PO') -- pitchout
    and a.batter_id=605141
    and a.season in('2021', '2022', '2023', '2024')