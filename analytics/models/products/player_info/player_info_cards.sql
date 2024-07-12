select
    a.player_id,
    b.fullname,
    a.team,
    a.division_name,
    c.positions,
    b.batside_desc,
    b.pitchhand_desc,
    b.height,
    b.weight,
    b.current_age,
    b.birthplace

from {{ ref('int_player_info__teams') }} a
    inner join {{ ref('int_player_info__attrs') }} b on a.player_id = b.player_id
    inner join {{ ref('int_player_info__positions') }} c on a.player_id = c.player_id

where a.player_id in(select player_id from {{ ref('included_players') }})
