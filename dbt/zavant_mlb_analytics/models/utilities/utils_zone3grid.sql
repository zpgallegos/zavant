-- 3x3 grid inside the zone, configurable

with recursive idx (i) as (
    select 0 as i
    union
    select i + 1 as i from idx where i + 1 < 3 -- set
)

select
    a.i as zone_row,
    b.i as zone_col,
    row_number() over (order by a.i, b.i) as zone_idx
from idx as a cross join idx as b
