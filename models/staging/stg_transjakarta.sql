WITH raw_data AS (
    SELECT * FROM {{ source('transjakarta', 'transjakarta_raw_full') }}
),

-- Profile every ID to see if it's a real station or a moving bus
station_profiles AS (
    SELECT 
        stop_id,
        COUNT(DISTINCT stop_name) AS distinct_name_count,
        MAX(stop_name) AS fallback_real_name
    FROM (
        -- Combine both tap in and tap out to get a true master list of IDs
        SELECT tapInStops AS stop_id, tapInStopsName AS stop_name FROM raw_data WHERE tapInStops IS NOT NULL
        UNION ALL
        SELECT tapOutStops, tapOutStopsName FROM raw_data WHERE tapOutStops IS NOT NULL
    ) combined_stops
    GROUP BY stop_id
),

renamed_and_cleaned AS (
    SELECT
        r.transID AS transaction_id,
        r.payCardID AS card_id,
        r.payCardBank AS card_bank_name,
        r.payCardName AS card_holder_name,
        r.payCardSex AS customer_gender,
        r.payCardBirthdate as customer_birthdate,
        COALESCE(CAST(r.payAmount AS INT), 0) AS payment_amount,

        CAST(r.tapInStopsLat AS DOUBLE) AS tap_in_latitude,
        CAST(r.tapInStopsLon AS DOUBLE) AS tap_in_longitude,

        CAST(r.tapOutStopsLat AS DOUBLE) AS tap_out_latitude,
        CAST(r.tapOutStopsLon AS DOUBLE) AS tap_out_longitude,

        COALESCE(r.corridorName, MAX(r.corridorName) OVER (PARTITION BY r.corridorID)) AS temp_corridor_name,
        COALESCE(r.corridorID, MAX(r.corridorID) OVER (PARTITION BY r.corridorName)) AS temp_corridor_id,

        r.direction,

        -- TAP IN: Apply Option 2 to force standard names
        r.tapInStops AS temp_tap_in_id,
        CASE 
            WHEN sp_in.distinct_name_count > 5 THEN 'Mikrotrans / Moving Route'
            ELSE COALESCE(r.tapInStopsName, sp_in.fallback_real_name)
        END AS temp_tap_in_name,

        r.stopStartSeq as tap_in_sequence,
        r.stopEndSeq as tap_out_sequence,

        -- TAP OUT: Apply Option 2 to force standard names
        r.tapOutStops AS temp_tap_out_id,
        CASE 
            WHEN sp_out.distinct_name_count > 5 THEN 'Mikrotrans / Moving Route'
            ELSE COALESCE(r.tapOutStopsName, sp_out.fallback_real_name)
        END AS temp_tap_out_name,

        CAST(r.tapInTime AS TIMESTAMP) AS tap_in_timestamp,
        CAST(r.tapOutTime AS TIMESTAMP) AS tap_out_timestamp

    FROM raw_data r
    -- Join our bouncer CTE to apply the rules
    LEFT JOIN station_profiles sp_in ON r.tapInStops = sp_in.stop_id
    LEFT JOIN station_profiles sp_out ON r.tapOutStops = sp_out.stop_id

    WHERE r.payCardID IS NOT NULL
),

deduplicated AS (
    SELECT 
        *,
        -- FIXED: Partition by transaction_id so we only drop duplicate identical swipes
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id 
            ORDER BY tap_in_timestamp DESC 
        ) as row_num
    FROM renamed_and_cleaned
), 

final_cleanup AS (
    SELECT
        CAST(transaction_id AS STRING) AS transaction_id,
        CAST(card_id AS STRING) AS card_id,
        CAST(customer_gender AS STRING) AS customer_gender,
        CAST(card_bank_name AS STRING) AS card_bank_name,
        CAST(card_holder_name AS STRING) AS card_holder_name,
        CAST(customer_birthdate AS INT) AS customer_birthdate,
        CAST(payment_amount AS INT) AS payment_amount,

        CAST(tap_in_latitude AS DOUBLE) AS tap_in_latitude,
        CAST(tap_in_longitude AS DOUBLE) AS tap_in_longitude,
        CAST(tap_out_latitude AS DOUBLE) AS tap_out_latitude,
        CAST(tap_out_longitude AS DOUBLE) AS tap_out_longitude,
        
        COALESCE(CAST(temp_corridor_name AS STRING) , 'Unknown Route') AS corridor_name,
        COALESCE(CAST(temp_corridor_id AS STRING), '-1') AS corridor_id,
        
        CAST(direction AS INT) AS direction,

        COALESCE(CAST(temp_tap_in_name AS STRING), 'Unknown Tap In Name') AS tap_in_name,
        TRIM(COALESCE(CAST(temp_tap_in_id AS STRING), '-1')) AS tap_in_id,

        CAST(tap_in_sequence AS INT) AS tap_in_sequence,
        CAST(tap_out_sequence AS INT) AS tap_out_sequence,

        COALESCE(CAST(temp_tap_out_name AS STRING), 'Unknown Tap out Name') AS tap_out_name,
        TRIM(COALESCE(CAST(temp_tap_out_id AS STRING), '-1')) AS tap_out_id,

        CAST(tap_in_timestamp AS TIMESTAMP) AS tap_in_timestamp,
        CAST(tap_out_timestamp AS TIMESTAMP) AS tap_out_timestamp

    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM final_cleanup