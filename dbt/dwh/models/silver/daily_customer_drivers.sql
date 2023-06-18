{{
    config(
        materialized='incremental',
        on_schema_change='fail',
        unique_key=['date','customerId']
    )
}}
WITH daily_customer_information AS (
    SELECT * FROM {{ ref('raw_daily_customer_drivers') }}
)
-- We impute some values to our columns
-- For customer with null monthly salary, we set the minimum vital salary (example: 1500)
-- As a rule on the bank customers who don't have score yet, must have 100 of health score
-- If current debt is null set to 0
SELECT
    date,
    customerId,
    COALESCE(monthly_salary, 1500) as monthlySalary, 
    CASE 
        WHEN health_score < 100 THEN 100
        ELSE COALESCE(health_score, 100)
    END healthScore, 
    COALESCE(current_debt, 0) as currentDebt,
    CASE 
        WHEN category IS NULL THEN  'NOT-CLASSIFIED'
        WHEN category = '' THEN 'NOT-CLASSIFIED'
        ELSE category
    END category,
    CASE 
        WHEN COALESCE(health_score, 100) < 100 THEN true
        ELSE false
    END isRiskyCustomer 
FROM daily_customer_information
WHERE date = '{{ var("target_date") }}'