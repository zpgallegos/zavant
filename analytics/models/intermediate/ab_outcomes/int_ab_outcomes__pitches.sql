with src as (
    select
        a.*,

        -- recode call description to group some similar calls together
        case
            when a.call_code in(
                '*B', -- Ball in Dirt
                'P'   -- Pitchout
            ) then 'Ball'
            when a.call_code in(
                'C', -- Called Strike
                'S', -- Swinging Strike
                'W', -- Swinging Strike (Blocked)
                'T', -- Foul Tip
                'O', -- Foul Tip (also...?)
                'L', -- Foul Bunt
                'M', -- Missed Bunt
                'R', -- Foul Pitchout
                'Q'  -- Swinging Pitchout
            ) then 'Strike'
        else a.call_desc
        end as call_desc_recode,

        -- single string for the count
        cast(a.count_balls as varchar) || '-' || cast(a.count_strikes as varchar) as end_count,

        -- mark last pitches, needed for filtering and encoding the play result
        case
            when row_number() over(partition by a.play_id order by a.event_idx desc) = 1 then 1
            else 0
        end as is_last_pitch
    from {{ ref('stg_statsapi__pitches') }} a
),
calcd1 as (
    select
        a.*,
        lag(a.end_count, 1) over(partition by a.play_id order by a.event_idx) as prev_count
    from src a
),
calcd2 as (
    select
        a.*,
        case
            when a.prev_count is null or a.end_count != a.prev_count then 1
            else 0
        end as count_changed
    from calcd1 a
),
erroneous as (
    -- plays where the data says the count reaches four balls or three strikes
    -- but the play doesn't end for some reason, data error

    select distinct a.play_id
    from src a
    where 
        1=1
        and a.is_last_pitch = 0
        and (a.count_balls = 4 or a.count_strikes = 3)
),
filtered as (
    select a.*
    from calcd2 a
    where
        1=1
        and not(
            -- fouls on two strikes, confirmed this is the only time where this condition happens
            a.count_changed = 0 and a.is_last_pitch = 0
        )
        and a.play_id not in(select play_id from erroneous)
)

select * from filtered