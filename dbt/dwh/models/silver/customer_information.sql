{{
    config(
        unique_key=['customerId']
    )
}}
WITH daily_customer_information AS (
    SELECT * 
    FROM {{ ref('raw_daily_customer_cdc') }}
)
SELECT
    date,
    customerId,
    firstName, 
    lastName, 
    phone, 
    email, 
    gender,
    address,
    is_active
FROM daily_customer_information
where {{ date_filter_batch('date') }}