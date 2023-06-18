{{
    config(
        materialized='incremental',
        on_schema_change='fail',
        unique_key=['date','customerId', 'paymentPeriod']
    )
}}
WITH daily_loan_transactions AS (
    SELECT * FROM {{ ref('raw_daily_loan_transactions') }}
)
SELECT
    date,
    customerId,
    paymentPeriod,
    CASE 
        WHEN loanAmount < 1000 THEN 1000
        ELSE loanAmount
    END loanAmount, 
    currencyType, 
    evaluationChannel, 
    interestRate
FROM daily_loan_transactions
WHERE date = '{{ var("target_date") }}'