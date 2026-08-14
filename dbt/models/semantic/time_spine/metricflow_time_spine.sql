{{
    config(
        materialized="table",
        table_type="iceberg",
        format="parquet"
    )
}}

-- Athena limits each sequence() result to 50,000 entries.
with date_ranges as (
    select
        date '1876-01-01' as range_start,
        date '1999-12-31' as range_end

    union all

    select
        date '2000-01-01' as range_start,
        date_add('year', 2, current_date) as range_end
)

select dates.date_day
from date_ranges
cross join unnest(
    sequence(
        date_ranges.range_start,
        date_ranges.range_end,
        interval '1' day
    )
) as dates (date_day)
