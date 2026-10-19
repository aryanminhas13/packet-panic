with waves as (
    select unnest(generate_series(1, 6)) as wave
), sessions as (
    select count(*) as total_sessions from {{ ref('fct_sessions') }}
)

select
    waves.wave,
    count(distinct f.session_id) as sessions_reached,
    round(100.0 * count(distinct f.session_id) / nullif(max(s.total_sessions), 0), 1) as reach_rate_pct
from waves
cross join sessions s
left join {{ ref('fct_sessions') }} f on f.max_wave >= waves.wave
group by waves.wave
order by waves.wave

