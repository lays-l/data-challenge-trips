with trips as (
    select *,
        CASE 
            WHEN EXTRACT(HOUR FROM departure_time) >= 0  AND EXTRACT(HOUR FROM departure_time) < 6  THEN 'Late Night'
            WHEN EXTRACT(HOUR FROM departure_time) >= 6  AND EXTRACT(HOUR FROM departure_time) < 12 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM departure_time) >= 12 AND EXTRACT(HOUR FROM departure_time) < 18 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM departure_time) >= 18 AND EXTRACT(HOUR FROM departure_time) < 24 THEN 'Evening'
        END AS time_of_day
    from {{ ref('stg_trips') }}
)
select
    origin_city,
    destination_city,
    time_of_day,
    count(*) as trip_count
from trips
group by origin_city, destination_city, time_of_day
order by origin_city, destination_city, time_of_day
