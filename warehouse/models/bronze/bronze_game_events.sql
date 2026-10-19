select
    event_id,
    schema_version,
    event_type,
    try_cast(client_time as timestamp) as client_time,
    try_cast(received_at as timestamp) as received_at,
    session_id,
    anonymous_player_id,
    sequence_number,
    game_version,
    wave,
    source,
    properties_json,
    batch_id,
    loaded_at
from {{ source('bronze', 'raw_game_events') }}

