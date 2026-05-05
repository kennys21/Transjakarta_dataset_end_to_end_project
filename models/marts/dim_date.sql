WITH raw_dates AS (
    -- Grab all dates from Tap-In
    SELECT CAST(tap_in_timestamp AS DATE) AS date_day 
    FROM {{ ref('stg_transjakarta') }}
    WHERE tap_in_timestamp IS NOT NULL

    UNION -- (A normal UNION automatically deduplicates, which is what we want here)

    -- Grab all dates from Tap-Out
    SELECT CAST(tap_out_timestamp AS DATE) AS date_day 
    FROM {{ ref('stg_transjakarta') }}
    WHERE tap_out_timestamp IS NOT NULL
)

SELECT
    date_day AS date_key,
    YEAR(date_day) AS year,
    MONTH(date_day) AS month,
    DAY(date_day) AS day,
    DATE_FORMAT(date_day, 'EEEE') AS day_name, 
    
    -- In Databricks, DAYOFWEEK returns 1 for Sunday and 7 for Saturday
    CASE 
        WHEN DAYOFWEEK(date_day) IN (1, 7) THEN True 
        ELSE False 
    END AS is_weekend

FROM raw_dates
WHERE date_day IS NOT NULL
ORDER BY date_key