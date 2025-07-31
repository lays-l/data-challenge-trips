with trips as (
    select * from {{ ref('stg_trips') }}
)
select
    region,
    date_trunc('week', departure_time) as week,
    count(*) as trip_count,
    count(*) / 7.0 as avg_trips_per_day
from trips
group by region, week
