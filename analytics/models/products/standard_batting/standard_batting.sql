with src as (
    select * from {{ ref('int_standard_batting__games') }} union
    select * from {{ ref('int_standard_batting__plate_appearances') }} union
    select * from {{ ref('int_standard_batting__countstats') }} union
    select * from {{ ref('int_standard_batting__strikeouts') }} union
    select * from {{ ref('int_standard_batting__rbi') }} union
    select * from {{ ref('int_standard_batting__runs') }} union
    select * from {{ ref('int_standard_batting__steals') }} union
    select * from {{ ref('int_standard_batting__caught_stealing') }}
),
pivoted as (
    select 
        a.player_id,
        a.season,
        a.complete,
        a.team_id,
        sum(case when a.stat = 'games_played' then value else 0 end) as G,
        sum(case when a.stat = 'plate_appearances' then value else 0 end) as PA,
        sum(case when a.stat = 'walk' then value else 0 end) as walks,
        sum(case when a.stat = 'single' then value else 0 end) as "1B",
        sum(case when a.stat = 'double' then value else 0 end) as "2B",
        sum(case when a.stat = 'triple' then value else 0 end) as "3B",
        sum(case when a.stat = 'home_run' then value else 0 end) as HR,
        sum(case when a.stat = 'rbi' then value else 0 end) as RBI,
        sum(case when a.stat = 'intent_walk' then value else 0 end) as IBB,
        sum(case when a.stat = 'hit_by_pitch' then value else 0 end) as HBP,
        sum(case when a.stat = 'sac_bunt' then value else 0 end) as SH,
        sum(case when a.stat = 'sac_fly' then value else 0 end) as SF,
        sum(case when a.stat = 'catcher_interf' then value else 0 end) as CI,
        sum(case when a.stat = 'grounded_into_double_play' then value else 0 end) as GIDP,
        sum(case when a.stat = 'strikeouts' then value else 0 end) as SO,
        sum(case when a.stat = 'runs' then value else 0 end) as R,
        sum(case when a.stat = 'stolen_bases' then value else 0 end) as SB,
        sum(case when a.stat = 'caught_stealing' then value else 0 end) as CS
    from src a
    group by 1, 2, 3, 4
),
calcd_1 as (
    select
        a.*,
        a.walks + a.IBB as BB,
        a."1B" + a."2B" + a."3B" + a.HR as H,
        a."1B" + 2 * a."2B" + 3 * a."3B" + 4 * a.HR as TB
    from pivoted a
),
calcd_2 as (
    select
        a.*,
        a.PA - a.BB - a.HBP - a.SH - a.SF - a.CI as AB
    from calcd_1 a
),
calcd_3 as (
    select
        a.*,
        cast(a.H as double) / cast(a.AB as double) as BA,
        cast(a.H + a.BB + a.HBP as double) / cast(a.AB + a.BB + a.HBP + a.SF as double) as OBP,
        cast(a.TB as double) / cast(a.AB as double) as SLG
    from calcd_2 a
    where a.AB > 0
),
calcd_4 as (
    select
        a.*,
        a.OBP + a.SLG as OPS
    from calcd_3 a
)

select
    a.player_id,
    a.season,
    a.complete,
    a.team_id,
    if(a.team_id = 0, 'TOT', b.team_short) as team_short,
    if(a.team_id = 0, 'MLB', if(b.league_name = 'National League', 'NL', 'AL')) as league_name_short,
    a.G,
    a.PA,
    a.AB,
    a.R,
    a.H,
    a."2B",
    a."3B",
    a.HR,
    a.RBI,
    a.SB,
    a.CS,
    a.BB,
    a.SO,
    a.BA,
    a.OBP,
    a.SLG,
    a.OPS,
    a.TB,
    a.GIDP,
    a.HBP,
    a.SH,
    a.SF,
    a.IBB

from calcd_4 a
    inner join {{ ref('stg_statsapi__player_info') }} c on a.player_id = c.player_id
    left join {{ ref('stg_statsapi__team_info') }} b on a.team_id = b.team_id