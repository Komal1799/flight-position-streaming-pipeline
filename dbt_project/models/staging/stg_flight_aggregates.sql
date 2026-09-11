-- Staging model: light cleanup of the raw flight_aggregates table
-- Why a staging layer: it isolates "raw source structure" from "business logic,"
-- so if the source table's schema ever changes, we only fix it in one place

select
    window_start,
    window_end,
    origin_country,
    aircraft_count,
    avg_velocity
from {{ source('flight_pipeline_source', 'flight_aggregates') }}
where aircraft_count > 0  -- filter out any degenerate/empty windows