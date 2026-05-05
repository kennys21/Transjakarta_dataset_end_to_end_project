with all_stations as(
    select
        tap_in_latitude as station_latitude,
        tap_in_longitude as station_longitude,
        tap_in_id as station_id,
        tap_in_name as station_name
        corridor_id,
        corridor_name
        
    from {{ref("stg_transjakarta")}}

    UNION ALL

    select
        
        tap_out_latitude as station_latitude,
        tap_out_longitude as station_longitude,
        tap_out_id as station_id,
        tap_out_name as station_name
        corridor_id,
        corridor_name
    from {{ref("stg_transjakarta")}}
),
deduplicated_station as(
    SELECT
        station_id,
        CASE 
            WHEN station_id = '-1' THEN 'Unknown/Glitch Station'
            ELSE MAX(station_name) 
        END AS station_name,
        MAX(corridor_id) as corridor_id,
        MAX(corridor_name) as corridor_name,
        MAX(station_latitude) AS station_latitude,
        MAX(station_longitude) AS station_longitude
    FROM all_stations
    WHERE station_id IS NOT NULL
    GROUP BY station_id
)

select * from deduplicated_station