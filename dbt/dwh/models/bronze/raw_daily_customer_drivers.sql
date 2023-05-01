{{
    config(
        materialized='view',
        on_schema_change='fail',
    )
}}

WITH raw_daily_customer_drivers AS (
    SELECT 
        *
    FROM {{ source('dwh', 'airbyte_daily_customer_drivers') }}
)
SELECT
    cast(date as date) as date,
    customerId,
    cast(monthly_salary as float) as monthly_salary, 
    health_score, 
    current_debt, 
    category
FROM raw_daily_customer_drivers