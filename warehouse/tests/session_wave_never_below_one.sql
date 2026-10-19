select *
from {{ ref('stg_game_events') }}
where wave < 1

