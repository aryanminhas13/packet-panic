with ranked as (
    select
        *,
        row_number() over (
            partition by event_id
            order by received_at asc, loaded_at asc
        ) as duplicate_rank
    from {{ ref('bronze_game_events') }}
)

select
    event_id,
    schema_version,
    event_type,
    client_time,
    received_at,
    session_id,
    anonymous_player_id,
    sequence_number,
    game_version,
    wave,
    source,
    properties_json,
    batch_id,
    datediff('second', client_time, received_at) as ingestion_lag_seconds
from ranked
where duplicate_rank = 1

