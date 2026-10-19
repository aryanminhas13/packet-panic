select
    session_id,
    anonymous_player_id,
    session_started_at,
    session_ended_at,
    duration_seconds,
    max_wave,
    coalesce(final_score, 0) as final_score,
    event_count,
    shots_fired,
    clean_packets_collected,
    corrupt_packets_destroyed,
    damage_events,
    completed_session,
    max_ingestion_lag_seconds,
    round(100.0 * corrupt_packets_destroyed / nullif(corrupt_packets_destroyed + damage_events, 0), 1) as intercept_rate_pct
from {{ ref('int_player_sessions') }}

