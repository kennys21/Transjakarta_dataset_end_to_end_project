WITH raw_data AS (
    SELECT * FROM {{ source('transjakarta', 'transjakarta_raw_full') }}
),

renamed_and_cleaned AS (
    SELECT
    payCardID AS card_id,
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
        card_id,
        
        
        COALESCE(temp_corridor_name, 'Unknown Route') AS corridor_name,
        COALESCE(temp_corridor_id, '-1') AS corridor_id,

        direction,

        COALESCE(temp_tap_in_name, 'Unknown Tap In Name') AS tap_in_name,
        COALESCE(temp_tap_in_id, '-1') AS tap_in_id,

        tap_in_sequence,
        tap_out_sequence,

        COALESCE(temp_tap_out_name, 'Unknown Tap out Name') AS tap_out_name,
        COALESCE(temp_tap_out_id, '-1') AS tap_out_id,

        tap_in_timestamp,
        tap_out_timestamp

        
    FROM deduplicated

    WHERE row_num = 1
)

SELECT * FROM final_cleanup
    
