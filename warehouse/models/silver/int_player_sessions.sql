select
    session_id,
    any_value(anonymous_player_id) as anonymous_player_id,
    min(client_time) as session_started_at,
    max(client_time) as session_ended_at,
    datediff('second', min(client_time), max(client_time)) as duration_seconds,
    max(wave) as max_wave,
    max(try_cast(json_extract_string(properties_json, '$.score') as integer)) as final_score,
    count(*) as event_count,
    count(*) filter (where event_type = 'pulse_fired') as shots_fired,
    count(*) filter (where event_type = 'clean_collected') as clean_packets_collected,
    count(*) filter (where event_type = 'corrupt_destroyed') as corrupt_packets_destroyed,
    count(*) filter (where event_type in ('damage_taken', 'corrupt_leaked')) as damage_events,
    count(*) filter (where event_type = 'session_ended') > 0 as completed_session,
    max(ingestion_lag_seconds) as max_ingestion_lag_seconds
from {{ ref('stg_game_events') }}
group by session_id

