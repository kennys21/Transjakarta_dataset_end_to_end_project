with raw_transaction as(
    Select
        transaction_id,
        card_id,
        customer_gender,
        CAST(payment_amount AS INT) as payment_amount,
        CAST(tap_in_timestamp AS DATE) AS date_key,
        tap_in_id,
        tap_out_id
    from 
        {{ref("stg_transjakarta")}}
    where 
        transaction_id IS NOT NULL
)

select * from raw_transaction