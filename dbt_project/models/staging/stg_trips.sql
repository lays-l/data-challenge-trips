with source as (
    select * from trips
)
select
    cast(region as varchar) as region,
    cast(origin_coord as varchar) as origin_coord,
    cast(destination_coord as varchar) as destination_coord,
    cast(departure_time as timestamp) as departure_time,
    cast(datasource as varchar) as datasource,
    cast(origin_city as varchar) as origin_city,
    cast(origin_country as varchar) as origin_country,
    cast(destination_city as varchar) as destination_city,
    cast(destination_country as varchar) as destination_country,
    cast(origin_latitude as float) as origin_latitude,
    cast(origin_longitude as float) as origin_longitude,
    cast(destination_latitude as float) as destination_latitude,
    cast(destination_longitude as float) as destination_longitude
from source