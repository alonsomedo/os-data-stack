{{
    config(
        materialized='incremental',
        on_schema_change='fail',
        unique_key=['date','customerId']
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
{% if is_incremental() %}
    WHERE date = '{{ var("target_date") }}'
{% endif %}