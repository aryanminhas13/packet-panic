with starts as (
    select session_id, min(client_time) as first_event_time
    from {{ ref('stg_game_events') }}
    where event_type = 'session_started'
    group by session_id
)

select events.*
from {{ ref('stg_game_events') }} events
left join starts using (session_id)
where starts.session_id is null
   or events.client_time < starts.first_event_time

