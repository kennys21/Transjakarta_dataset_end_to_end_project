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
        payAmount as payment_amount
    FROM raw_data

    WHERE transID IS NOT NULL 
),

deduplicated AS (
    SELECT 
        *,
        -- This partitions the data by the ID. If there are 3 identical transaction_ids, 
        -- it numbers them 1, 2, and 3 so it can flag the duplicated data 
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id 
            ORDER BY transaction_id 
        ) as row_num
    FROM renamed_and_cleaned
)

SELECT 
    transaction_id,
    card_id,
    card_bank_name,
    card_holder_name,
    customer_gender,
    customer_birthdate,
    payment_amount
FROM deduplicated
WHERE row_num = 1
-- row_num = 1 makesure no duplicate data