{{
    config(
        materialized='view',
        on_schema_change='fail',
    )
}}

WITH raw_daily_customer_cdc AS (
    SELECT 
        *
    FROM {{ source('dwh', 'airbyte_daily_customer_information_cdc') }}
)
SELECT
    cast(date as date) as date,
    customerId,
    firstName, 
    lastName, 
    phone, 
    email, 
    gender,
    address,
    is_active
FROM raw_daily_customer_cdc