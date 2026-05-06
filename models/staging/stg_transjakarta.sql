WITH raw_data AS (
    SELECT * FROM {{ source('transjakarta', 'transjakarta_raw_full') }}
),

renamed_and_cleaned AS (
    SELECT

    transID AS transaction_id,
    payCardID AS card_id,
    payCardBank AS card_bank_name,
    payCardName AS card_holder_name,
    payCardSex AS customer_gender,
    payCardBirthdate as customer_birthdate,
    COALESCE(CAST(payAmount AS INT), 0) AS payment_amount,

    CAST(tapInStopsLat AS DOUBLE) AS tap_in_latitude,
    CAST(tapInStopsLon AS DOUBLE) AS tap_in_longitude,

    CAST(tapOutStopsLat AS DOUBLE) AS tap_out_latitude,
    CAST(tapOutStopsLon AS DOUBLE) AS tap_out_longitude,

    COALESCE(corridorName, MAX(corridorName) OVER (PARTITION BY corridorID)) AS temp_corridor_name,
    COALESCE(corridorID, MAX(corridorID) OVER (PARTITION BY corridorName)) AS temp_corridor_id,

    direction,

    COALESCE(tapInStops, MAX(tapInStops) OVER (PARTITION BY tapInStopsName)) as temp_tap_in_id,
    COALESCE(tapInStopsName, MAX(tapInStopsName) OVER (PARTITION BY tapInStops)) as temp_tap_in_name,

    stopStartSeq as tap_in_sequence,
    stopEndSeq as tap_out_sequence,

    COALESCE(tapOutStops, MAX(tapOutStops) OVER (PARTITION BY tapOutStopsName)) as temp_tap_out_id,
    COALESCE(tapOutStopsName, MAX(tapOutStopsName) OVER (PARTITION BY tapOutStops)) as temp_tap_out_name,

    CAST(tapInTime AS TIMESTAMP) AS tap_in_timestamp,
    CAST(tapOutTime AS TIMESTAMP) AS tap_out_timestamp


    FROM raw_data

    WHERE payCardID IS NOT NULL
),

deduplicated AS (
    SELECT 
        *,
        -- This partitions the data by the ID. If there are 3 identical transaction_ids, 
        -- it numbers them 1, 2, and 3 so it can flag the duplicated data 
        ROW_NUMBER() OVER (
            PARTITION BY card_id 
            ORDER BY card_id 
        ) as row_num
    FROM renamed_and_cleaned
), 

final_cleanup AS (
    SELECT

        CAST(transaction_id AS VARCHAR) AS transaction_id,
        CAST(card_id AS VARCHAR) AS card_id,
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
        COALESCE(CAST(temp_corridor_id AS VARCHAR), '-1') AS corridor_id,
        

        CAST(direction AS INT) AS direction,

        COALESCE(CAST(temp_tap_in_name AS STRING), 'Unknown Tap In Name') AS tap_in_name,
        COALESCE(CAST(temp_tap_in_id AS VARCHAR), '-1') AS tap_in_id,

        CAST(tap_in_sequence AS INT) AS tap_in_sequence,
        CAST(tap_out_sequence AS INT) AS tap_out_sequence,

        COALESCE(CAST(temp_tap_out_name AS STRING), 'Unknown Tap out Name') AS tap_out_name,
        COALESCE(CAST(temp_tap_out_id AS VARCHAR), '-1') AS tap_out_id,

        CAST(tap_in_timestamp AS TIMESTAMP) AS tap_in_timestamp,
        CAST(tap_out_timestamp AS TIMESTAMP) AS tap_out_timestamp

        
    FROM deduplicated

    WHERE row_num = 1
)

SELECT * FROM final_cleanup
    
SELECT
        

        

        