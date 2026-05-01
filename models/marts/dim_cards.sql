select
    card_id,
    card_bank_name,
    customer_birthdate,
    customer_gender
from {{ref("stg_transjakarta")}}