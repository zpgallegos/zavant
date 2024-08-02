with src as (
    select 
        a.*,
        case
        when row_number() over(partition by a.game_pk order by a.play_idx desc) = 1 then 1
        else 0
        end as is_last_play
    from {{ ref('stg_statsapi__play_info') }} a
),
filtered as (
    select a.*
    from src a
    where
        1=1

        -- game ends on an incomplete play for a game advisory (rain or whatever)
        and a.is_complete

        -- at bat ends with another play, batter would be coming back up next inning
        -- if these don't end the inning, they're not recorded as the play result
        and not (
            a.count_outs = 3 and (
                a.play_result_code like 'pickoff%' or
                a.play_result_code like 'caught%' or
                a.play_result_code like 'other_out%'
            )
        )

        -- whole game ends on a non-AB ending play
        and not (
            a.is_last_play = 1 and (
                a.play_result_code like '%wild_pitch%' or
                a.play_result_code like '%passed_ball%' or
                a.play_result_code like '%balk%'
            )
        )

        -- other unwanted play results for this product's purpose
        -- generally, we only want plays that directly involve the batter
        and a.play_result_code not in(
            'game_advisory',
            'catcher_interf',
            'hit_by_pitch'
        )
)

select * from filtered
