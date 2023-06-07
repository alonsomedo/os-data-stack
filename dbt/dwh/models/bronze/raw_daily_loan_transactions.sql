{{
    config(
        materialized='view',
        on_schema_change='fail',
    )
}}

WITH raw_daily_loan_transactions AS (
    SELECT 
        *
    FROM {{ source('dwh', 'airbyte_daily_loan_transactions') }}
)
SELECT
    cast(date as date) as date,
    customerId,
    paymentPeriod,
    cast(loanAmount as float) as loanAmount, 
    currencyType, 
    evaluationChannel, 
    cast(interest_rate as float) as interestRate
FROM raw_daily_loan_transactions