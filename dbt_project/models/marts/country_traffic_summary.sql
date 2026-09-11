-- Marts model: aggregates staging data into a country-level traffic summary
-- Why this exists: raw windowed data is granular (1-minute buckets);
-- this rolls it up into something a dashboard or analyst would actually query

select
    origin_country,
    sum(aircraft_count) as total_aircraft_observed,
    round(avg(avg_velocity)::numeric, 2) as avg_velocity_overall,
    max(aircraft_count) as peak_aircraft_in_single_window,
    count(*) as windows_observed
from {{ ref('stg_flight_aggregates') }}
group by origin_country
order by total_aircraft_observed desc