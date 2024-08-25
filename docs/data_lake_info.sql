with cte as (
    select 
        (select count(1) from zavant.play_events) +
        (select count(1) from zavant.play_runners) as event_count,
        (select count(1) from zavant.play_info) as play_count,
        (select count(1) from zavant.game_info) as game_count
)

select * from cte;
