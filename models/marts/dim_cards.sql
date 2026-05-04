with deduplicated_cards as(
    select
        card_id,
        max(card_bank_name) as card_bank_name,
        max(customer_birthdate) as customer_birthdate,
    from 
        {{ref("stg_transjakarta")}}
    where  
        card_id is not null
    group by 
        card_id
)

select * from deduplicated_cards